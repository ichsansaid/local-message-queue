from concurrent.futures import Future, Executor
from typing import Any, Callable, List, Type

from balancer.hash_based_balancer import HashBasedBalancer
from consumer import Consumer
from messagebroker import MessageBroker
from producer import Producer
from topic_config import TopicConfig


class SingleCenteredTopicMessageQueue:
    def __init__(
            self, 
            topic: str, 
            callback: Callable[[Consumer, Any, Any], Any],
            thread_pool: Executor,
            num_threads: int = 2,
    ):
        self.num_threads = num_threads
        self.callback = callback
        self.topic = topic
        self.broker = MessageBroker(HashBasedBalancer)
        self.broker.create_topic(self.topic, TopicConfig(
            total_threads=self.num_threads
        ))
        self.producer = Producer(self.broker)
        self.consumers = [
            Consumer(self.broker, num_thread=p+1, id=p+1, thread_process=thread_pool) for p in range(self.num_threads)
        ]
        self.threads = []

    def start(self) -> List[Future]:
        self.threads = [consumer.pull(self.topic, self.callback) for consumer in self.consumers]
        return self.threads

    def push(self, value: Any, key: Any = None):
        self.producer.push(self.topic, value, key)





