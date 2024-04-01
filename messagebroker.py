import typing
from concurrent.futures import Executor, ThreadPoolExecutor
from queue import Queue

from balancer.balancer_base import BalancerBase
from topic_config import TopicConfig


class MessageBroker:
    def __init__(
            self,
            balancer_class: typing.Type[BalancerBase[Queue]],
    ):
        self.queue: typing.Dict[str, typing.List[Queue]] = {}
        self.topics: typing.Dict[str, TopicConfig] = {}
        self.key_iterator: typing.Dict[str, int] = {}
        self.balancer_class = balancer_class
        self.balancers: typing.Dict[str, BalancerBase[Queue] | None] = {}

    def create_topic(self, topic: str, config: TopicConfig):
        self.topics[topic] = config
        self.queue[topic] = []
        for num_thread in range(config.total_threads):
            self.queue[topic].append(Queue())
            self.balancers[topic] = self.balancer_class(self.queue[topic])
        self.key_iterator[topic] = 0

    def push(self, topic: str, value: typing.Any, key: typing.Any = None):
        if topic not in self.queue:
            raise Exception("Topic not found in Message Broker")
        queue = self.balancers[topic].next(key)
        queue.put((value, key), block=False)