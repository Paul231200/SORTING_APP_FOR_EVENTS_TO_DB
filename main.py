import os
import pika
import redis
import json
import uuid
import threading
from clickhouse_connect import get_client
from datetime import datetime, timedelta
import time
import pytz  # Добавляем для работы с часовыми поясами

# Настройки из переменных окружения
REDIS_HOST = os.getenv("REDIS_HOST", "")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
AMQP_URL = os.getenv("AMQP_URL")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "")
CLICKHOUSE_USER = os.getenv("DB_USER", "")
CLICKHOUSE_PASSWORD = os.getenv("DB_PASS", "")
CLICKHOUSE_DATABASE = os.getenv("DB_DATABASE", "")
MAX_OPERATION_TIME = int(os.getenv("MAX_OPERATION_TIME", 24))  # часов
STALE_CHECK_INTERVAL = int(os.getenv("STALE_CHECK_INTERVAL", 300))  # секунд
# Maximum reasonable operation duration in minutes before we consider it suspicious
MAX_REASONABLE_DURATION = int(os.getenv("MAX_REASONABLE_DURATION", 240))  # 4 hours default
# Время проверки операций после окончания рабочего времени (в минутах)
AFTER_HOURS_OPERATION_CHECK = int(os.getenv("AFTER_HOURS_OPERATION_CHECK", 30))

# Операции, которые игнорируются
IGNORED_OPERATIONS = ["unwind", "unwind_pause", "rolling", "roll_in_place"]

# Инициализация клиентов Redis и ClickHouse
redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

clickhouse_client = get_client(
    host=CLICKHOUSE_HOST,
    port=8123,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE
)

# Глобальные переменные для хранения состояний
current_states = {}

def log(msg):
    """Функция для логирования с временной меткой"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def format_datetime(dt):
    """Форматирование даты/времени для ClickHouse"""
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if dt else None

def parse_datetime(dt_str):
    """Разбор строки даты/времени"""
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except:
        return datetime.now()

def get_valid_datetime(timestamp):
    """Преобразование timestamp в datetime"""
    try:
        if isinstance(timestamp, str):
            timestamp = float(timestamp)
        if timestamp > 1e12:  # если в миллисекундах
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp)
    except:
        return datetime.now()

def insert_to_clickhouse(uuid, zone, operation, date_start, date_end):
    """Вставка операции в ClickHouse"""
    ts = int(datetime.now().timestamp() * 1000)
    query = """
    INSERT INTO spk_iot.ml_operations (uuid, puuid, device, date_start, date_end, operation, speed, pieces, ts)
    VALUES (%(uuid)s, NULL, %(device)s, %(date_start)s, %(date_end)s, %(operation)s, NULL, NULL, %(ts)s)
    """
    values = {
        "uuid": uuid,
        "device": zone,
        "date_start": format_datetime(date_start),
        "date_end": format_datetime(date_end),
        "operation": operation,
        "ts": ts
    }
    clickhouse_client.query(query, parameters=values)
    log(f"Данные записаны в ClickHouse: uuid={uuid}, zone={zone}, operation={operation}, start={values['date_start']}, end={values['date_end']}")

def get_active_operations(zone, event=None):
    """Получение активных операций из Redis"""
    operations = []
    for key in redis_client.keys("*"):
        try:
            data = json.loads(redis_client.get(key))
            if data["zone"] == zone and not data.get("date_end"):
                if event is None or data["operation"] == event:
                    operations.append(data)
        except:
            continue
    return operations

def is_working_hours():
    """Проверяет, входит ли текущее время в рабочие часы (8:00-19:00 по Екатеринбургу)"""
    yekaterinburg_tz = pytz.timezone('Asia/Yekaterinburg')
    current_time = datetime.now(yekaterinburg_tz)
    start_time = current_time.replace(hour=8, minute=0, second=0, microsecond=0)
    end_time = current_time.replace(hour=19, minute=0, second=0, microsecond=0)
    return start_time <= current_time <= end_time

def check_stale_operations():
    """Поток для проверки и закрытия зависших операций"""
    while True:
        try:
            log("Проверка зависших операций...")
            working_hours = is_working_hours()
            
            # Текущее время в Екатеринбурге для логирования
            yekaterinburg_tz = pytz.timezone('Asia/Yekaterinburg')
            current_time_ekb = datetime.now(yekaterinburg_tz)
            log(f"Текущее время (Екатеринбург): {current_time_ekb.strftime('%Y-%m-%d %H:%M:%S')} - {'рабочее время' if working_hours else 'нерабочее время'}")
            
            for key in redis_client.keys("*"):
                try:
                    data = json.loads(redis_client.get(key))
                    if not data.get("date_end"):
                        date_start = parse_datetime(data["date_start"])
                        current_duration_hours = (datetime.now() - date_start).total_seconds() / 3600
                        
                        # Проверка на операции, выходящие за рабочее время
                        close_after_hours = False
                        if not working_hours and data.get("operation") not in IGNORED_OPERATIONS:
                            # Если нерабочее время, и операция продолжается более AFTER_HOURS_OPERATION_CHECK минут после окончания рабочего дня
                            end_of_workday = current_time_ekb.replace(hour=19, minute=0, second=0, microsecond=0)
                            if end_of_workday.day == current_time_ekb.day:  # Если сегодня был рабочий день
                                minutes_after_work = (current_time_ekb - end_of_workday).total_seconds() / 60
                                if minutes_after_work > AFTER_HOURS_OPERATION_CHECK:
                                    close_after_hours = True
                                    log(f"Операция продолжается после рабочего времени: {data['uuid']}, zone={data['zone']}, "
                                        f"operation={data['operation']}, время после окончания рабочего дня: {minutes_after_work:.2f} минут")
                        
                        # Check if operation has been running too long
                        if current_duration_hours > MAX_OPERATION_TIME or close_after_hours:
                            reason = "превышен MAX_OPERATION_TIME" if current_duration_hours > MAX_OPERATION_TIME else "операция за пределами рабочего времени"
                            log(f"Закрываем зависшую операцию ({reason}): {data['uuid']}, zone={data['zone']}, "
                                f"operation={data['operation']}, длительность={current_duration_hours:.2f} часов")
                            
                            # Время окончания - либо сейчас, либо конец рабочего дня для операций вне рабочего времени
                            end_time = datetime.now()
                            if close_after_hours:
                                # Используем конец рабочего дня как время завершения операции
                                end_time = current_time_ekb.replace(hour=19, minute=0, second=0, microsecond=0).replace(tzinfo=None)
                            
                            insert_to_clickhouse(
                                data["uuid"], data["zone"], data["operation"], date_start, end_time
                            )
                            redis_client.delete(key)
                            current_states[data["zone"]] = "0"
                        
                        # Check for reasonable duration for non-ignored operations
                        elif current_duration_hours > (MAX_REASONABLE_DURATION / 60) and data.get("operation") not in IGNORED_OPERATIONS:
                            # Log warning about suspiciously long operation
                            log(f"Предупреждение: обнаружена подозрительно длинная операция: {data['uuid']}, zone={data['zone']}, "
                                f"operation={data['operation']}, длительность={current_duration_hours:.2f} часов")
                except Exception as e:
                    log(f"Ошибка при проверке зависшей операции {key}: {e}")
        except Exception as e:
            log(f"Ошибка в проверке зависших операций: {e}")
        time.sleep(STALE_CHECK_INTERVAL)

def main():
    """Основная функция программы"""
    log("Запуск обработчика событий...")
    parameters = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    exchange = "ml_events"
    routing_key = "ml_event"
    queue_name = "event_processing_queue"

    channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routing_key)
    
    # Закрываем незавершенные операции от предыдущего запуска
    try:
        log("Очистка незавершенных операций от предыдущего запуска...")
        for key in redis_client.keys("*"):
            try:
                data = json.loads(redis_client.get(key))
                if not data.get("date_end"):
                    date_start = parse_datetime(data["date_start"])
                    insert_to_clickhouse(
                        data["uuid"], data["zone"], data["operation"], 
                        date_start, datetime.now()
                    )
                    redis_client.delete(key)
                    log(f"Закрыта незавершенная операция: {data['uuid']}, zone={data['zone']}, operation={data['operation']}")
            except Exception as e:
                log(f"Ошибка при очистке операции {key}: {e}")
    except Exception as e:
        log(f"Ошибка при очистке операций: {e}")

    def callback(ch, method, properties, body):
        """Обработчик событий из очереди"""
        try:
            message = json.loads(body)
            zone = message.get("zone")
            event = message.get("event")
            event_state = str(message.get("event_state"))
            timestamp = message.get("timestamp")
            
            # Проверка корректности сообщения
            if not zone or event_state is None or not event:
                log(f"Пропуск сообщения с неполными данными: {message}")
                return

            # Игнорируем определенные операции
            if event in IGNORED_OPERATIONS:
                return

            event_time = get_valid_datetime(timestamp)
            previous_state = current_states.get(zone, "0")
            current_time = datetime.now()
            if event_state == "1":  # Начало операции
                # Проверяем, нет ли уже активной операции этого типа
                active_ops = get_active_operations(zone, event)
                if active_ops:
                    log(f"Операция {event} уже активна для zone={zone}, пропуск создания дубликата")
                    return
                
                # Создаем новую операцию
                operation_uuid = str(uuid.uuid4())
                data = {
                    "uuid": operation_uuid,
                    "zone": zone,
                    "operation": event,
                    "date_start": format_datetime(event_time),
                    "date_end": None
                }
                redis_client.set(operation_uuid, json.dumps(data))
                insert_to_clickhouse(operation_uuid, zone, event, event_time, None)
                current_states[zone] = event_state  # Update current state
                log(f"Операция начата: uuid={operation_uuid}, zone={zone}, event={event}")
                
            elif event_state == "0":  # Завершение операции
                # Ищем активные операции
                active_ops = get_active_operations(zone, event)
                if active_ops:
                    # Завершаем все найденные операции
                    active_ops.sort(key=lambda op: parse_datetime(op["date_start"]))
                    # Close all active operations of this type, not just the oldest
                    for op in active_ops:
                        date_start = parse_datetime(op["date_start"])
                        # Check for unreasonably long operations
                        duration_hours = (event_time - date_start).total_seconds() / 3600
                        if duration_hours > (MAX_REASONABLE_DURATION / 60):
                            log(f"Предупреждение: закрытие подозрительно длинной операции {event}: uuid={op['uuid']}, zone={zone}, длительность={duration_hours:.2f} часов")
                        
                        insert_to_clickhouse(op["uuid"], zone, event, date_start, event_time)
                        redis_client.delete(op["uuid"])
                        log(f"Операция завершена: uuid={op['uuid']}, zone={zone}, event={event}")
                else:
                    # Only create synthetic event if previous state was "1" (helps avoid false data)
                    if previous_state == "1":
                        operation_uuid = str(uuid.uuid4())
                        estimated_start = event_time - timedelta(minutes=1)
                        insert_to_clickhouse(operation_uuid, zone, event, estimated_start, event_time)
                        log(f"Операция завершена без активного начала: uuid={operation_uuid}, zone={zone}, event={event}")
                current_states[zone] = "0"

        except Exception as e:
            log(f"Ошибка в callback: {e}")

    # Запускаем проверку зависших операций в отдельном потоке
    stale_thread = threading.Thread(target=check_stale_operations, daemon=True)
    stale_thread.start()
    
    # Начинаем слушать сообщения
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    log("Ожидание сообщений...")
    channel.start_consuming()

if __name__ == "__main__":
    main()
