import asyncio
import datetime as dt
import logging
import traceback
from typing import Awaitable, Callable, Optional

import aio_pika

from .exceptions import MQHandlerRegistrationError, MQNotInitializedError

# Инициализируем логгер для пакета.
# Пользователь пакета сможет настроить его через logging.basicConfig()
logger = logging.getLogger(__name__)


class MQConsumer:
    """
    Асинхронный и надежный RabbitMQ Consumer.
    Обрабатывает получение сообщений, реализует отложенные повторные попытки (Delayed Messages)
    и перенаправление в Dead Letter Queue (DLQ) при необрабатываемых ошибках.
    """

    def __init__(
        self,
        connection_string: str,
        handler: Callable[[str], Awaitable[None]],
        queue_name: str,
        exchange_name: str,
        exchange_type: str = "direct",  # Тип основного обменника
        routing_key: Optional[str] = None,  # Ключ маршрутизации для связывания очереди
        prefetch_count: int = 1,  # QoS: сколько сообщений консьюмер может взять за раз
        dlx_routing_key: str = "dlq_routing_key",  # Ключ для маршрутизации в DLQ
    ):
        """
        Инициализирует MQConsumer.

        :param connection_string: Строка подключения к RabbitMQ (например, "amqp://guest:guest@localhost:5672/").
        :param handler: Асинхронная функция, которая будет обрабатывать сообщения.
        :param queue_name: Имя основной очереди, из которой будут потребляться сообщения.
        :param exchange_name: Имя основного обменника, к которому будет привязана очередь.
        :param exchange_type: Тип основного обменника (по умолчанию 'direct', также 'topic', 'fanout' и т.д.).
                              Для отложенных сообщений он будет обернут в 'x-delayed-message'.
        :param routing_key: Ключ маршрутизации для связывания основной очереди с основным обменником.
                            По умолчанию равен `queue_name`. Для 'topic' exchange необходим.
        :param prefetch_count: Количество сообщений, которое RabbitMQ отправит консьюмеру за раз.
                               Обеспечивает справедливое распределение нагрузки.
        :param dlx_routing_key: Ключ маршрутизации, используемый при перенаправлении сообщений в DLQ.
        """
        self.connection_string = connection_string
        self.handler = handler
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.routing_key = routing_key if routing_key is not None else queue_name
        self.prefetch_count = prefetch_count
        self.dlx_routing_key = dlx_routing_key

        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None
        self.dlx_exchange: Optional[aio_pika.Exchange] = None
        self.queue: Optional[aio_pika.Queue] = None
        self._is_running = False
        self._consumer_task: Optional[asyncio.Task] = None

    async def _setup_infrastructure(self):
        """
        Объявляет все необходимые Exchange, Queues и Bindings в RabbitMQ.
        Этот метод вызывается автоматически при запуске консьюмера.
        """
        if not self.channel:
            raise MQNotInitializedError(
                "Channel is not available during infrastructure setup."
            )

        logger.debug(
            "Declaring exchange '%s' of type 'x-delayed-message' wrapping '%s'.",
            self.exchange_name,
            self.exchange_type,
        )
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name,
            type="x-delayed-message",
            durable=True,
            arguments={"x-delayed-type": self.exchange_type},
        )

        dlx_exchange_name = f"{self.exchange_name}.dlx"
        dlq_name = f"{self.queue_name}.dlq"

        logger.debug("Declaring Dead Letter Exchange '%s'.", dlx_exchange_name)
        self.dlx_exchange = await self.channel.declare_exchange(
            dlx_exchange_name, type="direct", durable=True
        )

        logger.debug("Declaring main queue '%s' with DLX arguments.", self.queue_name)
        self.queue = await self.channel.declare_queue(
            self.queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": dlx_exchange_name,
                "x-dead-letter-routing-key": self.dlx_routing_key,
            },
        )

        logger.debug(
            "Binding queue '%s' to exchange '%s' with routing key '%s'.",
            self.queue_name,
            self.exchange_name,
            self.routing_key,
        )
        await self.queue.bind(self.exchange, routing_key=self.routing_key)

        logger.debug(
            "Declaring Dead Letter Queue '%s' and binding to DLX with key '%s'.",
            dlq_name,
            self.dlx_routing_key,
        )
        dlq = await self.channel.declare_queue(dlq_name, durable=True)
        await dlq.bind(self.dlx_exchange, routing_key=self.dlx_routing_key)

    async def run(self):
        """
        Подключается к RabbitMQ, настраивает необходимую инфраструктуру
        и начинает асинхронное потребление сообщений.
        Этот метод будет работать, пока не будет вызван `stop()`.
        """
        if self.handler is None:
            raise MQHandlerRegistrationError(
                "No message handler registered. Use @consumer.message_handler to decorate a function "
                "before calling run()."
            )

        logger.info("Connecting to RabbitMQ")
        self.connection = await aio_pika.connect_robust(self.connection_string)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.prefetch_count)

        await self._setup_infrastructure()

        logger.info("Starting message consumption from queue '%s'.", self.queue_name)
        if not self.queue:
            raise MQNotInitializedError(
                "Queue object is not initialized for consumption."
            )

        self._is_running = True

        # Запускаем основной цикл потребления в фоновой задаче
        self._consumer_task = asyncio.create_task(self._consume_loop())

        # Ждем завершения задачи, чтобы run() был блокирующим до остановки
        await self._consumer_task

    async def _consume_loop(self):
        """Внутренний цикл потребления сообщений."""
        try:
            async with self.queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self._is_running:
                        logger.info(
                            "Consumer stopped. Breaking message consumption loop."
                        )
                        break
                    # Запускаем обработку каждого сообщения в отдельной задаче
                    asyncio.create_task(self._safe_process_message(message))
        except asyncio.CancelledError:
            logger.info(
                "Message consumption loop cancelled for queue '%s'.", self.queue_name
            )
        except Exception as e:
            logger.error(
                "Critical error in consumer loop for queue '%s': %s",
                self.queue_name,
                e,
                exc_info=True,
            )
        finally:
            logger.info("Exiting consumer loop for queue '%s'.", self.queue_name)

    async def stop(self):
        """
        Грациозно останавливает консьюмера.
        Завершает текущую задачу потребления и закрывает соединение с RabbitMQ.
        """
        logger.info("Stopping MQConsumer for queue '%s'...", self.queue_name)
        self._is_running = False
        if self._consumer_task:
            self._consumer_task.cancel()  # Отменяем задачу потребления
            try:
                await self._consumer_task  # Ждем её завершения
            except asyncio.CancelledError:
                pass  # Ожидаемое исключение при отмене

        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("RabbitMQ connection closed for queue '%s'.", self.queue_name)

    async def _safe_process_message(self, message: aio_pika.IncomingMessage):
        """
        Обрабатывает отдельное сообщение:
        - Декодирует его тело в строку.
        - Передает пользовательскому обработчику.
        - При успехе подтверждает сообщение.
        - При ошибке логирует её, подтверждает сообщение и отправляет в DLQ.
        """
        if self.handler is None:
            logger.error(
                "No user handler defined for message processing in _safe_process_message."
            )
            await message.nack(requeue=True)
            return

        message_body_str = message.body.decode("utf-8")
        try:
            logger.debug(
                "Processing message from '%s': %s", self.queue_name, message_body_str
            )
            await self.handler(message_body_str)
            await message.ack()
            logger.debug(
                "Message successfully processed and acknowledged from '%s'.",
                self.queue_name,
            )
        except Exception as e:
            tb_str = traceback.format_exc()
            logger.error(
                "Error processing message from queue '%s'. Sending to DLQ. Message: %s, Error: %s\n%s",
                self.queue_name,
                message_body_str,
                e,
                tb_str,
            )
            await message.ack()
            await self._publish_to_dlq(message, e, tb_str)

    async def _publish_to_dlq(
        self,
        original_message: aio_pika.IncomingMessage,
        error: Exception,
        traceback_str: str,
    ):
        """
        Публикует сообщение, которое не удалось обработать, в Dead Letter Exchange.
        Добавляет информацию об ошибке в заголовки сообщения.
        """
        if not self.dlx_exchange:
            raise MQNotInitializedError(
                "DLX is not initialized, cannot publish failed message to DLQ."
            )

        # Копируем исходные заголовки и добавляем свои детали об ошибке
        headers = original_message.info().get("headers", {}) or {}
        headers["x-exception-details"] = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "traceback": traceback_str,
            "failed_at": dt.datetime.now().isoformat(),
        }

        # Публикуем сообщение в DLX
        await self.dlx_exchange.publish(
            aio_pika.Message(
                body=original_message.body,
                headers=headers,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type=original_message.content_type,
                content_encoding=original_message.content_encoding,
                correlation_id=original_message.correlation_id,
            ),
            routing_key=self.dlx_routing_key,
        )
        logger.info(
            "Message sent to DLQ for queue '%s' with key '%s'.",
            self.queue_name,
            self.dlx_routing_key,
        )
