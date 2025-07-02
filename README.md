# `simple-rabbitmq`: Простая асинхронная библиотека для работы с RabbitMQ

`simple-rabbitmq` — это легкая и функциональная асинхронная библиотека на Python, разработанная для упрощения взаимодействия с брокером сообщений RabbitMQ. Она абстрагирует низкоуровневые детали `aio-pika`, предоставляя высокоуровневый API для публикации и потребления сообщений, включая поддержку продвинутых паттернов, таких как отложенные сообщения (Delayed Messages) и очереди мертвых писем (Dead Letter Queue).

## Особенности

*   **Простая интеграция:** Минималистичный API для быстрого подключения к RabbitMQ.
*   **Асинхронность:** Полностью асинхронная архитектура на базе `asyncio` и `aio-pika`.
*   **Надежность:** Встроенная поддержка `RobustConnection` для автоматического переподключения к RabbitMQ.
*   **Отложенные сообщения:** Публикация сообщений с заданной задержкой (требует плагина `rabbitmq_delayed_message_exchange`).
*   **Очереди мертвых писем (DLQ):** Автоматическое перенаправление сообщений, которые не удалось обработать, в DLQ для последующего анализа и отладки.
*   **Гибкая конфигурация:** Настраиваемые типы Exchange, ключи маршрутизации и параметры очередей.
*   **Развязка:** Publisher не знает о существовании очередей, Consumer сам объявляет свою инфраструктуру.

## Установка

```bash
pip install aio-pika  # Основная зависимость
# Возможно, вам также потребуется установить ваш пакет локально, пока он не опубликован
# pip install -e .  # Если вы в корне вашего проекта и есть setup.py/pyproject.toml
```

**Требования к RabbitMQ:**
Для использования функционала отложенных сообщений (`x-delayed-message` Exchange) необходимо убедиться, что на вашем RabbitMQ сервере активирован плагин `rabbitmq_delayed_message_exchange`.

Пример активации для Docker-контейнера:
```bash
docker exec -it <your_rabbitmq_container_name> rabbitmq-plugins enable rabbitmq_delayed_message_exchange
```

## Использование

### `MQPublisher` (Публикация сообщений)

```python
import asyncio
import logging
import json
from simple-rabbitmq import MQPublisher, MQConnectionError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def main():
    publisher = MQPublisher(
        connection_string="amqp://guest:guest@localhost:5672/",
        exchange_name="my_app_exchange",
        exchange_type="topic", # Может быть 'direct', 'topic', 'fanout'
        durable=True,
        delayed_exchange_type="topic" # Тип для внутренней отложенной обработки
    )

    try:
        await publisher.connect()

        # Публикация простого сообщения
        await publisher.publish(
            message_data="Hello from my_app!",
            routing_key="app.status.info"
        )
        logging.info("Sent: Simple info message.")

        # Публикация JSON-объекта
        user_data = {"user_id": 42, "event": "signup", "timestamp": "2023-10-27T10:00:00Z"}
        await publisher.publish(
            message_data=user_data,
            routing_key="app.user.signup"
        )
        logging.info("Sent: User signup event (JSON).")

        # Публикация сообщения с задержкой (например, для отложенной задачи)
        delayed_task = {"task_name": "send_welcome_email", "user_id": 42}
        await publisher.publish(
            message_data=delayed_task,
            routing_key="app.task.delayed",
            delay_ms=5000 # Сообщение будет доставлено через 5 секунд
        )
        logging.info("Sent: Delayed task message.")

    except MQConnectionError as e:
        logging.error(f"Publisher failed: {e}")
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if publisher:
            await publisher.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### `MQConsumer` (Потребление сообщений)

```python
import asyncio
import logging
import json
from simple-rabbitmq import MQConsumer, MQHandlerRegistrationError, MQConnectionError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def main():

    async def process_message(message_body: str):
        """
        Пример асинхронной функции, обрабатывающей сообщение.
        Если здесь произойдет исключение, сообщение будет отправлено в DLQ.
        """
        logging.info(f"CONSUMER: Received: {message_body}")
        try:
            data = json.loads(message_body)
            if data.get("event") == "signup" and data.get("user_id") == 42:
                logging.info(f"CONSUMER: Processing signup for user {data['user_id']}")
                # Имитация длительной операции
                await asyncio.sleep(2)
                if random.random() < 0.1: # 10% шанс на ошибку
                    raise ValueError("Simulated processing error for signup!")
            logging.info(f"CONSUMER: Finished processing: {message_body}")
        except json.JSONDecodeError:
            logging.error(f"CONSUMER: Failed to decode JSON: {message_body}")
            raise # Перевыбрасываем, чтобы отправить в DLQ
        except Exception as e:
            logging.error(f"CONSUMER: Unhandled error in handler: {e}", exc_info=True)
            raise # Перевыбрасываем, чтобы отправить в DLQ

    consumer = MQConsumer(
        connection_string="amqp://guest:guest@localhost:5672/",
        handler=process_message,
        queue_name="my_app_queue",
        exchange_name="my_app_exchange",
        exchange_type="topic", # Должен совпадать с типом Exchange, куда публикует Publisher
        routing_key="app.#", # Подписываемся на все сообщения, начинающиеся с "app."
        prefetch_count=1 # Обрабатывать по одному сообщению за раз
    )

    try:
        # Запуск консьюмера. Он будет работать, пока не будет остановлен (например, Ctrl+C)
        logging.info("Starting consumer...")
        await consumer.run() 
    except MQConnectionError as e:
        logging.critical(f"Consumer failed: {e}", exc_info=True)
    except MQHandlerRegistrationError as e:
        logging.critical(f"Handler setup error: {e}")
    except asyncio.CancelledError:
        logging.info("Consumer stopped gracefully.")
    except Exception as e:
        logging.critical(f"An unexpected error occurred in Consumer: {e}", exc_info=True)
    finally:
        if consumer:
            await consumer.stop()
            logging.info("Consumer connection closed.")

if __name__ == "__main__":
    import random
    asyncio.run(main())
```

## Обработка ошибок и DLQ

Если функция-обработчик в `MQConsumer` выбрасывает исключение, сообщение автоматически будет отправлено в Dead Letter Exchange (DLX) и связанную с ним Dead Letter Queue (DLQ). Это позволяет не терять "битые" сообщения и анализировать причины сбоев.

По умолчанию, DLX будет называться `<exchange_name>.dlx` (например, `my_app_exchange.dlx`), а DLQ - `<queue_name>.dlq` (например, `my_app_queue.dlq`), с `dlx_routing_key` по умолчанию `dlq_routing_key`. Вы можете настроить эти имена при инициализации `MQConsumer`.

## Кастомные исключения

Библиотека использует собственные исключения для более специфичной обработки ошибок:
*   `MQConnectionError`: Общая ошибка подключения к MQ.
*   `MQNotInitializedError`: Операция вызвана до установления соединения.
*   `MQHandlerRegistrationError`: Ошибка при регистрации обработчика сообщений.
