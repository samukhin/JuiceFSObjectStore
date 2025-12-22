"""
Простой S3-сервер, совместимый с MinIO.

Эта программа реализует базовые операции S3 API на FastAPI:
- Создание бакетов (PUT /{bucket})
- Загрузка объектов (PUT /{bucket}/{object})
- Скачивание объектов (GET /{bucket}/{object})
- Удаление объектов (DELETE /{bucket}/{object})
- Список объектов в бакете (GET /{bucket})

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
lock = asyncio.Lock()
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
    xml_parts = [
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
        etag = hashlib.md5(data).hexdigest()
        # Получаем текущее время в UTC для LastModified
        utc_now = datetime.now(timezone.utc)
        last_modified = utc_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        size = len(data)
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
async def create_bucket(bucket: str):
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
        buckets[bucket]  # Инициализируем пустой словарь для объектов
    return Response(status_code=200)


@app.get("/{bucket}")
async def list_objects(
    bucket: str,
    list_type: str | None = None,
    prefix: str = "",
):
    """
    Возвращает список объектов в указанном бакете в формате S3 XML.
    Если бакет не найден, возвращает ошибку 404.
    """
    async with lock:
        if bucket not in buckets:
            raise HTTPException(status_code=404, detail="Bucket not found")
        # Фильтруем объекты по префиксу
        filtered_objects = [
            obj for obj in buckets[bucket].keys() if obj.startswith(prefix)
        ]
        # Генерируем XML
        xml_response = generate_bucket_list_xml(
            bucket, prefix, filtered_objects, buckets[bucket]
        )
    return Response(content=xml_response, media_type="application/xml")


@app.put("/{bucket}/{obj:path}")
async def put_object(bucket: str, obj: str, request: Request):
    """
    Загружает объект в бакет.
    Читает тело запроса как данные объекта и сохраняет в памяти.
    Если бакет не найден, возвращает ошибку 404.
    Возвращает ETag в заголовке.
    """
    data = await request.body()  # Асинхронно читаем тело запроса
    async with lock:
        if bucket not in buckets:
            raise HTTPException(status_code=404, detail="Bucket not found")
        buckets[bucket][obj] = data  # Сохраняем данные объекта
    # Вычисляем ETag как MD5 хэш данных
    etag = hashlib.md5(data).hexdigest()
    return Response(status_code=200, headers={"ETag": f'"{etag}"'})


@app.get("/{bucket}/{obj:path}")
async def get_object(bucket: str, obj: str):
    """
    Скачивает объект из бакета.
    Возвращает данные как поток с заголовком для скачивания.
    Если объект или бакет не найдены, возвращает ошибку 404.
    """
    async with lock:
        if bucket not in buckets or obj not in buckets[bucket]:
            raise HTTPException(status_code=404, detail="Object not found")
        data = buckets[bucket][obj]  # Получаем данные объекта
    etag = hashlib.md5(data).hexdigest()
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
async def delete_object(bucket: str, obj: str):
    """
    Удаляет объект из бакета.
    Если объект или бакет не найдены, возвращает ошибку 404.
    """
    async with lock:
        if bucket not in buckets or obj not in buckets[bucket]:
            raise HTTPException(status_code=404, detail="Object not found")
        del buckets[bucket][obj]  # Удаляем объект из словаря
    return {"message": "Object deleted"}


def parse_arguments():
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
        default="0.0.0.0",
        help="Хост для запуска сервера (по умолчанию: 0.0.0.0)",
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
    args = parse_arguments()
    debug = args.debug
    host = args.host
    port = args.port

    if debug:
        # Настраиваем логирование
        logging.basicConfig(level=logging.DEBUG, format="DEBUG: %(message)s")
        logging.debug("Режим отладки включен.")

        # Добавляем middleware для логирования запросов
        from starlette.middleware.base import BaseHTTPMiddleware

        class LoggingMiddleware(BaseHTTPMiddleware):
            """Middleware для логирования запросов и ответов в debug режиме."""

            def __init__(self, app):
                super().__init__(app)

            def get_name(self):
                """Возвращает имя middleware."""
                return "LoggingMiddleware"

            async def dispatch(self, request, call_next):
                logging.debug(f"Запрос: {request.method} {request.url}")
                try:
                    response = await call_next(request)
                    logging.debug(f"Ответ: {response.status_code}")
                    return response
                except Exception as e:
                    logging.debug(f"Ошибка: {e}")
                    raise

        app.add_middleware(LoggingMiddleware)
    else:
        # Отключаем лишние логи, если не debug
        logging.getLogger().setLevel(logging.WARNING)

    # Запускаем сервер
    uvicorn.run(app, host=host, port=port)
    # Запускаем сервер с указанными host и port
