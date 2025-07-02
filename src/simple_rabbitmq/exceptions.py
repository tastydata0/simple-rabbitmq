
# Определяем кастомные исключения для более явной обработки ошибок
class MQConnectionError(Exception):
    """Базовое исключение для проблем с подключением к MQ."""

    pass


class MQNotInitializedError(MQConnectionError):
    """Возникает, если операция вызывается до инициализации соединения/каналов."""

    pass


class MQHandlerRegistrationError(MQConnectionError):
    """Возникает при ошибках регистрации обработчика."""

    pass
