import json
import logging
from typing import Any, Dict, Optional, Union

import aio_pika

from .exceptions import MQConnectionError, MQNotInitializedError

# Инициализируем логгер для пакета
logger = logging.getLogger(__name__)


class MQPublisher:
    """
    Асинхронный RabbitMQ Publisher, обеспечивающий надежную отправку сообщений
    в указанный Exchange, включая поддержку отложенных сообщений.
    """

    def __init__(
        self,
        connection_string: str,
        exchange_name: str,
        # 1. Убираем queue_name из конструктора, publisher не должен знать об очередях
        exchange_type: str = "direct",  # Тип основного обменника
        durable: bool = True,  # Должен ли Exchange быть durable
        delayed_exchange_type: str = "direct",  # Тип внутреннего delayed-message exchange
    ):
        """
        Инициализирует MQPublisher.

        :param connection_string: Строка подключения к RabbitMQ.
        :param exchange_name: Имя Exchange, в который будут публиковаться сообщения.
        :param exchange_type: Тип Exchange (например, 'direct', 'topic', 'fanout').
                              Этот Exchange будет обернут в 'x-delayed-message'.
        :param durable: Должен ли Exchange быть durable (переживать перезапуск брокера).
        :param delayed_exchange_type: Внутренний тип для 'x-delayed-message' exchange.
                                      Обычно 'direct', 'topic' или 'fanout'.
        """
        self.connection_string = connection_string
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.durable = durable
        self.delayed_exchange_type = delayed_exchange_type

        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None

    async def connect(self):
        """
        Устанавливает соединение с RabbitMQ, создает канал и объявляет Exchange.
        Этот метод должен быть вызван перед публикацией сообщений.
        """
        logger.info("Connecting to RabbitMQ")
        self.connection = await aio_pika.connect_robust(self.connection_string)
        self.channel = await self.connection.channel()

        # 2. Объявляем Exchange с возможностью задержки, но с настраиваемым типом
        logger.info(
            "Declaring exchange '%s' of type 'x-delayed-message' wrapping '%s'.",
            self.exchange_name,
            self.delayed_exchange_type,
        )
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name,
            type="x-delayed-message",
            durable=self.durable,
            arguments={"x-delayed-type": self.delayed_exchange_type},
        )

    async def close(self):
        """
        Грациозно закрывает соединение с RabbitMQ.
        """
        logger.info(
            "Closing MQPublisher connection for exchange '%s'...", self.exchange_name
        )
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info(
                "RabbitMQ connection closed for exchange '%s'.", self.exchange_name
            )

    async def publish(
        self,
        message_data: Union[str, Dict[str, Any]],  # Теперь принимаем str или dict
        routing_key: str,  # Routing key теперь обязательный аргумент публикации
        delay_ms: int = 0,
        delivery_mode: aio_pika.DeliveryMode = aio_pika.DeliveryMode.PERSISTENT,  # Персистентность по умолчанию
        properties: Optional[Dict[str, Any]] = None,  # Дополнительные заголовки
        mandatory: bool = True,  # Оставляем mandatory для уведомления о недоставке
    ):
        """
        Публикует сообщение в RabbitMQ Exchange.

        :param message_data: Данные сообщения. Может быть строкой или словарем (будет сериализован в JSON).
        :param routing_key: Ключ маршрутизации, используемый Exchange для направления сообщения.
        :param delay_ms: Задержка в миллисекундах перед доставкой сообщения. (0 для немедленной).
        :param delivery_mode: Режим доставки сообщения (PERSISTENT по умолчанию для надежности).
        :param properties: Дополнительные заголовки сообщения.
        :param mandatory: Если True, брокер вернет сообщение, если его невозможно маршрутизировать.
                          Требует обработки 'return_listener'.
        :raises MQNotInitializedError: Если соединение не установлено.
        :raises ValueError: Если message_data имеет неподдерживаемый тип.
        """
        if not self.exchange:
            raise MQNotInitializedError(
                "Publisher is not connected to RabbitMQ. Call .connect() first."
            )

        # 4. Обработка входящих данных и сериализация
        body: bytes
        if isinstance(message_data, str):
            body = message_data.encode("utf-8")
        elif isinstance(message_data, dict):
            body = json.dumps(message_data).encode("utf-8")
        else:
            raise ValueError("Message data must be a string or a dictionary.")

        headers_to_send = properties if properties is not None else {}
        if delay_ms > 0:
            headers_to_send["x-delay"] = delay_ms

        message = aio_pika.Message(
            body=body,
            headers=headers_to_send,
            delivery_mode=delivery_mode,
            content_type=(
                "application/json" if isinstance(message_data, dict) else "text/plain"
            ),
            # Можно добавить correlation_id, message_id и т.д. если нужно
        )

        logger.info(
            "Publishing message to exchange '%s' with routing key '%s' (delay: %dms).",
            self.exchange_name,
            routing_key,
            delay_ms,
        )
        try:
            # publish возвращает asyncio.Future, если включены подтверждения
            await self.exchange.publish(
                message, routing_key=routing_key, mandatory=mandatory
            )
            logger.debug("Message published successfully.")
        except aio_pika.exceptions.NackError:
            logger.error(
                "Message was Nacked by RabbitMQ for routing_key '%s'.", routing_key
            )
            # Здесь можно добавить логику повторной отправки или обработки отказа
        except aio_pika.exceptions.ChannelClosed:
            logger.error(
                "Channel closed while publishing to routing_key '%s'.", routing_key
            )
            raise MQConnectionError("RabbitMQ channel closed during publish.") from None
        except Exception as e:
            logger.critical(
                "Failed to publish message to routing_key '%s': %s",
                routing_key,
                e,
                exc_info=True,
            )
            raise  # Перевыбрасываем для обработки выше
