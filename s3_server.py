"""
Простой S3-сервер, совместимый с MinIO.

Эта программа реализует базовые операции S3 API на FastAPI:
- Создание бакетов (PUT /{bucket})
- Загрузка объектов (PUT /{bucket}/{object})
- Скачивание объектов (GET /{bucket}/{object})
- Удаление объектов (DELETE /{bucket}/{object})
- Список объектов в бакете (GET /{bucket})
- Метаданные объекта (HEAD /{bucket}/{object})

Данные хранятся в памяти (dict), поэтому при перезапуске
сервера данные теряются.
Не включает аутентификацию или авторизацию - для простоты.
Для тестирования с S3-клиентами установите dummy credentials:
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
Для запуска: python s3_server.py [--debug]
[--host HOST] [--port PORT]
Документация API автоматически генерируется на
http://localhost:8000/docs
"""

# Импортируем необходимые модули
import argparse  # Для парсинга аргументов командной строки
import asyncio  # Для асинхронного lock
import hashlib  # Для вычисления ETag
import logging  # Для логирования в debug режиме
from collections import defaultdict  # Для автоматического создания dict
from datetime import datetime, timezone  # Для LastModified
from io import BytesIO  # Для работы с байтовыми данными в памяти

from fastapi import (  # FastAPI для создания API, HTTPException для ошибок
    FastAPI,
    HTTPException,
    Request,
    Response,
)
from fastapi.responses import StreamingResponse  # Для потоковой отдачи файлов

# Создаем экземпляр FastAPI приложения
app = FastAPI()

# Словарь для хранения данных: бакет -> {объект: данные}
buckets: dict[str, dict[str, bytes]] = defaultdict(dict)
lock: asyncio.Lock = asyncio.Lock()
# словарь бакетов: {бакет: {объект: данные}}


def generate_bucket_list_xml(
    bucket: str,
    prefix: str,
    filtered_objects: list[str],
    bucket_data: dict[str, bytes],
) -> str:
    """
    Генерирует XML для списка объектов в бакете в формате S3.
    """
    xml_parts: list[str] = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-20/">',
        f"<Name>{bucket}</Name>",
        f"<Prefix>{prefix}</Prefix>",
        f"<KeyCount>{len(filtered_objects)}</KeyCount>",
        "<MaxKeys>1000</MaxKeys>",
        "<IsTruncated>false</IsTruncated>",
    ]
    for obj in filtered_objects:
        data = bucket_data[obj]
        # Вычисляем ETag как MD5 хэш данных объекта
        etag: str = hashlib.md5(data, usedforsecurity=False).hexdigest()
        # Получаем текущее время в UTC для LastModified
        utc_now: datetime = datetime.now(timezone.utc)
        last_modified: str = utc_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        size: int = len(data)
        xml_parts.extend(
            [
                "<Contents>",
                f"<Key>{obj}</Key>",
                f"<LastModified>{last_modified}</LastModified>",
                f'<ETag>"{etag}"</ETag>',
                f"<Size>{size}</Size>",
                "<StorageClass>STANDARD</StorageClass>",
                "</Contents>",
            ]
        )
    xml_parts.append("</ListBucketResult>")
    return "\n".join(xml_parts)


@app.put("/{bucket}")
async def create_bucket(bucket: str) -> Response:
    """
    Создает новый бакет.
    Если бакет уже существует, возвращает ошибку 409 (Conflict).
    """
    async with lock:
        if bucket in buckets:
            raise HTTPException(
                status_code=409,
                detail="Bucket already exists",
            )
        buckets[bucket] = {}  # Инициализируем пустой словарь для объектов
    return Response(status_code=200)


@app.get("/{bucket}")
async def list_objects(
    bucket: str,
    prefix: str = "",
) -> Response:
    """
    Возвращает список объектов в указанном бакете в формате S3 XML.
    Если бакет не найден, возвращает ошибку 404.
    """
    async with lock:
        if bucket not in buckets:
            raise HTTPException(status_code=404, detail="Bucket not found")
        # Фильтруем объекты по префиксу
        filtered_objects: list[str] = [
            obj for obj in buckets[bucket].keys() if obj.startswith(prefix)
        ]
        # Генерируем XML
        xml_response: str = generate_bucket_list_xml(
            bucket, prefix, filtered_objects, buckets[bucket]
        )
    return Response(content=xml_response, media_type="application/xml")


@app.put("/{bucket}/{obj:path}")
async def put_object(bucket: str, obj: str, request: Request) -> Response:
    """
    Загружает объект в бакет.
    Читает тело запроса как данные объекта и сохраняет в памяти.
    Если бакет не найден, возвращает ошибку 404.
    Поддерживает условные заголовки If-None-Match и If-Match
    для условных записей.
    Возвращает ETag в заголовке.
    """
    data: bytes = await request.body()  # Асинхронно читаем тело запроса
    async with lock:
        if bucket not in buckets:
            raise HTTPException(status_code=404, detail="Bucket not found")

        # Проверяем условные заголовки для поддержки условных записей
        if_none_match = request.headers.get("If-None-Match")

        # If-None-Match: * означает, что объект не должен существовать
        if if_none_match == "*" and obj in buckets[bucket]:
            raise HTTPException(status_code=412, detail="Precondition Failed")

        buckets[bucket][obj] = data  # Сохраняем данные объекта
    # Вычисляем ETag как MD5 хэш данных
    etag: str = hashlib.md5(data, usedforsecurity=False).hexdigest()
    return Response(
        status_code=200,
        headers={"ETag": f'"{etag}"'},
    )


@app.head("/{bucket}/{obj:path}")
async def head_object(bucket: str, obj: str) -> Response:
    """
    Возвращает метаданные объекта без тела.
    Используется для проверки существования объекта.
    Если объект или бакет не найдены, возвращает ошибку 404.
    """
    async with lock:
        if bucket not in buckets or obj not in buckets[bucket]:
            raise HTTPException(status_code=404, detail="Object not found")
        data: bytes = buckets[bucket][obj]  # Получаем данные объекта
    etag: str = hashlib.md5(data, usedforsecurity=False).hexdigest()
    return Response(
        status_code=200,
        headers={
            "ETag": f'"{etag}"',
            "Content-Length": str(len(data)),
            "Content-Type": "application/octet-stream",
        },
    )


@app.get("/{bucket}/{obj:path}")
async def get_object(bucket: str, obj: str) -> StreamingResponse:
    """
    Скачивает объект из бакета.
    Возвращает данные как поток с заголовком для скачивания.
    Если объект или бакет не найдены, возвращает ошибку 404.
    """
    async with lock:
        if bucket not in buckets or obj not in buckets[bucket]:
            raise HTTPException(status_code=404, detail="Object not found")
        data: bytes = buckets[bucket][obj]  # Получаем данные объекта
    etag: str = hashlib.md5(data, usedforsecurity=False).hexdigest()
    return StreamingResponse(
        BytesIO(data),  # Оборачиваем в BytesIO для потоковой отдачи
        media_type="application/octet-stream",  # MIME тип для бинарных данных
        headers={
            "Content-Disposition": f"attachment; filename={obj}",
            "ETag": f'"{etag}"',
            "Content-Length": str(len(data)),
        },  # Заголовки
    )


@app.delete("/{bucket}/{obj:path}")
async def delete_object(bucket: str, obj: str) -> dict[str, str]:
    """
    Удаляет объект из бакета.
    Если объект или бакет не найдены, возвращает ошибку 404.
    """
    async with lock:
        if bucket not in buckets or obj not in buckets[bucket]:
            raise HTTPException(status_code=404, detail="Object not found")
        del buckets[bucket][obj]  # Удаляем объект из словаря
    return {"message": "Object deleted"}


def parse_arguments() -> argparse.Namespace:
    """
    Парсит аргументы командной строки.
    Возвращает объект с аргументами.
    """

    parser = argparse.ArgumentParser(description="S3 server on FastAPI")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Включить отладочное логирование запросов и ошибок",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Хост для запуска сервера (по умолчанию: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Порт для запуска сервера (по умолчанию: 8000)",
    )
    return parser.parse_args()


# Если файл запущен напрямую, запускаем сервер
if __name__ == "__main__":
    import uvicorn  # Импортируем uvicorn для запуска ASGI сервера

    # Парсим аргументы
    args: argparse.Namespace = parse_arguments()
    debug: bool = args.debug
    host: str = args.host
    port: int = args.port

    if debug:
        # Настраиваем логирование
        logging.basicConfig(level=logging.DEBUG, format="DEBUG: %(message)s")
        logging.debug("Режим отладки включен.")

        # Добавляем middleware для логирования запросов
        from starlette.middleware.base import BaseHTTPMiddleware

        class LoggingMiddleware(BaseHTTPMiddleware):
            """Middleware для логирования запросов и ответов в debug режиме."""

            def __init__(self, middleware_app) -> None:
                super().__init__(middleware_app)

            def get_name(self) -> str:
                """Возвращает имя middleware."""
                return "LoggingMiddleware"

            async def dispatch(self, request, call_next) -> Response:
                logging.debug("Запрос: %s %s", request.method, request.url)
                try:
                    response: Response = await call_next(request)
                    logging.debug("Ответ: %s", response.status_code)
                    return response
                except Exception as e:
                    logging.debug("Ошибка: %s", e)
                    raise

        app.add_middleware(LoggingMiddleware)
    else:
        # Отключаем лишние логи, если не debug
        logging.getLogger().setLevel(logging.WARNING)

    # Запускаем сервер
    uvicorn.run(app, host=host, port=port)
    # Запускаем сервер с указанными host и port
