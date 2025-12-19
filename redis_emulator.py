"""
Минимальный эмулятор Redis для JuiceFS Object Storage.
Поддерживает основные команды: GET, SET, DEL, EXISTS, STRLEN, SCAN, PING, HELLO, CLUSTER.
"""

import asyncio, re, logging, argparse, signal  # Импорты для asyncio, regex, логирования и аргументов командной строки
from typing import Any

storage: dict[bytes, bytes] = {}  # Хранилище ключ-значение в памяти (bytes для поддержки бинарных данных)
lock = asyncio.Lock()  # Асинхронный лок для потокобезопасности операций с storage

parser = argparse.ArgumentParser()  # Парсер аргументов командной строки
_ = parser.add_argument('--debug', action='store_true')  # Флаг для включения отладочного логирования
args = parser.parse_args()
logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)  # Настройка логирования
logger = logging.getLogger()

def resp(data: Any) -> bytes:
    """
    Сериализует данные в формат RESP (Redis Serialization Protocol).
    Обрабатывает None, bytes, str, int, list; возвращает bytes.
    """
    if data is None: return b"$-1\r\n"  # Null значение
    if isinstance(data, bytes): return f"${len(data)}\r\n".encode() + data + b"\r\n"  # Бинарная строка
    if isinstance(data, str): return resp(data.encode())  # Строку конвертируем в bytes
    if isinstance(data, int): return f":{data}\r\n".encode()  # Целое число
    if isinstance(data, list): return f"*{len(data)}\r\n".encode() + b"".join(resp(i) for i in data)  # Массив
    return b"-ERR\r\n"  # Ошибка для неизвестного типа

def parse(data: bytes):
    """
    Парсит массив RESP, возвращает аргументы команды и количество потребленных байт.
    Формат: *count\r\n$len\r\nvalue\r\n... (проверяет полную команду).
    """
    if not data.startswith(b"*"): return [], 0  # Не массив - игнорируем
    try:
        end = data.find(b"\r\n")  # Ищем конец заголовка массива
        count = int(data[1:end])  # Количество аргументов
        args, pos = [], end + 2  # Список аргументов и текущая позиция
        for _ in range(count):  # Парсим каждый аргумент
            if pos >= len(data) or data[pos] != ord("$"): return [], 0  # Ожидаем $ для строки
            dollar_end = data.find(b"\r\n", pos)  # Конец длины
            length = int(data[pos+1:dollar_end])  # Длина значения
            value_start = dollar_end + 2  # Начало значения
            value_end = value_start + length  # Конец значения
            if value_end + 2 > len(data): return [], 0  # Недостаточно данных
            args.append(data[value_start:value_end])  # Добавляем аргумент
            pos = value_end + 2  # Сдвигаем позицию
        return args, pos  # Возвращаем аргументы и потребленные байты
    except (ValueError, IndexError): return [], 0  # Ошибка парсинга

async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """
    Обрабатывает соединение одного клиента.
    Читает данные, накапливает в буфер, парсит полные команды, выполняет и отвечает.
    """
    buffer = b""  # Буфер для накопления данных (RESP может приходить частями)
    try:
        while data := await reader.read(1024):  # Читаем данные порциями
            buffer += data  # Накопление буфера
            while buffer:  # Пока есть данные в буфере
                cmd_args, consumed = parse(buffer)  # Парсим команду
                if not consumed: break  # Недостаточно данных для полной команды
                buffer = buffer[consumed:]  # Удаляем обработанную часть
                if args.debug: logger.debug(f"Cmd: {[a.decode(errors='ignore') for a in cmd_args]}")  # Лог команды в debug
                if not cmd_args: response = b"-ERR\r\n"  # Неверная команда
                else:
                    cmd = cmd_args[0].decode(errors='ignore').upper()  # Извлекаем команду
                    async with lock: response = cmd_handler(cmd, cmd_args)  # Выполняем с блокировкой
                writer.write(response)  # Отправляем ответ
                await writer.drain()  # Ждем отправки
    except Exception as e: logger.error(e)  # Лог ошибок
    finally: writer.close(); await writer.wait_closed()  # Закрываем соединение

def cmd_handler(cmd: str, args: list[bytes]) -> bytes:
    """
    Обрабатывает команду Redis.
    Реализует логику для каждой команды с проверками количества аргументов.
    """
    if cmd == "GET": return resp(storage.get(args[1])) if len(args) > 1 else b"-ERR\r\n"  # Получить значение
    if cmd == "SET": storage[args[1]] = args[2]; return b"+OK\r\n" if len(args) > 2 else b"-ERR\r\n"  # Установить значение
    if cmd == "DEL":  # Удалить ключи
        count = 0
        for k in args[1:]: count += 1 if storage.pop(k, None) else 0  # Считаем удаленные
        return resp(count)
    if cmd == "EXISTS": return resp(sum(1 for k in args[1:] if k in storage))  # Проверить существование
    if cmd == "STRLEN": return resp(len(storage.get(args[1], b""))) if len(args) > 1 else b"-ERR\r\n"  # Длина значения
    if cmd == "SCAN":  # Сканирование ключей
        cursor = int(args[1].decode(errors='ignore')) if len(args) > 1 else 0  # Текущий курсор
        match = args[3] if len(args) > 3 and args[2].decode(errors='ignore').upper() == "MATCH" else b"*"  # Паттерн
        count = int(args[5].decode(errors='ignore')) if len(args) > 5 and args[4].decode(errors='ignore').upper() == "COUNT" else 10  # Количество
        keys = list(storage.keys())[cursor:cursor+count]  # Выбираем ключи
        if match != b"*":  # Фильтр по паттерну
            pat = re.compile(match.decode(errors='ignore').replace("*", ".*"))
            keys = [k for k in keys if pat.match(k.decode(errors='ignore'))]
        new_cursor = 0 if cursor + len(keys) >= len(storage) else cursor + len(keys)  # Новый курсор
        return resp([str(new_cursor).encode(), keys])  # Ответ: курсор + ключи
    if cmd == "PING": return b"+PONG\r\n"  # Проверка соединения
    if cmd == "HELLO": return resp(["server", "redis", "version", "6.0.0", "proto", 3, "id", 1, "mode", "standalone", "role", "master", "modules", []])  # Информация о сервере
    if cmd == "CLUSTER": return b"-ERR This instance has cluster support disabled\r\n"  # Кластер отключен
    return b"-ERR\r\n"  # Неизвестная команда

async def main() -> None:
    """
    Основной цикл сервера.
    Запускает TCP сервер на порту 6379, обрабатывает сигналы завершения.
    """
    server = await asyncio.start_server(handle, '0.0.0.0', 6379)  # Создаем сервер
    logger.info("Redis emulator on port 6379")  # Лог запуска
    loop, stop = asyncio.get_running_loop(), asyncio.Event()  # Получаем event loop и событие остановки
    def shutdown(): logger.info("Stopping..."); stop.set()  # Функция остановки
    _ = loop.add_signal_handler(signal.SIGINT, shutdown)  # Обработчик Ctrl+C
    async with server:  # Контекстный менеджер для сервера
        await stop.wait()  # Ждем сигнала остановки
        server.close()  # Закрываем сервер
        await server.wait_closed()  # Ждем закрытия

if __name__ == "__main__": asyncio.run(main())  # Запуск main функции