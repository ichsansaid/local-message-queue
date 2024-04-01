from typing import Any

from messagebroker import MessageBroker


class Producer:
    def __init__(self, broker: MessageBroker):
        self.broker = broker

    def push(self, topic: str, value: Any = None, key: Any = None, ):
        self.broker.push(topic, key=key, value=value)
