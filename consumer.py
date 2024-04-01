import uuid
from concurrent.futures import Executor, ThreadPoolExecutor, Future
from threading import Thread
from typing import Callable, Type

from messagebroker import MessageBroker


def consumer_task(consumer: 'Consumer', broker: MessageBroker):
    while True:
        curr_queue = broker.queue[consumer.topic][consumer.num_thread].get()
        if curr_queue is None:
            break
        consumer.callback(consumer, value=curr_queue[0], key=None if len(curr_queue) == 1 else curr_queue[1])


class Consumer:
    def __init__(
            self,
            broker: MessageBroker,
            thread_process: Executor,
            num_thread: int | None = None,
            id=None,
    ):
        self.broker = broker
        self.thread_process: Executor = thread_process
        self.topic = None
        self.callback = None
        self.num_thread = num_thread-1
        self.id = id

    def pull(self, topic: str, callback: Callable) -> Future:
        self.topic = topic
        self.callback = callback
        return self.thread_process.submit(consumer_task, self, self.broker)
