import time
import timeit
from concurrent.futures import ThreadPoolExecutor, Executor, ProcessPoolExecutor
from itertools import chain
from threading import Thread

from balancer.hash_based_balancer import HashBasedBalancer
from consumer import Consumer
from messagebroker import MessageBroker
from producer import Producer
from topic_config import TopicConfig

broker = MessageBroker(HashBasedBalancer)
broker.create_topic("message", TopicConfig(
    total_threads=5
))
producer = Producer(broker)


def consumer_callback(consumer: Consumer, value, key):
    # Long task
    # print(f"Start Task of value ({value}) and key ({key}) at {consumer.id}")
    pass


def consumer_thread(pool_thread):
    consumers = [
        Consumer(broker, num_thread=1, id=1, thread_process=pool_thread),
        Consumer(broker, num_thread=2, id=2, thread_process=pool_thread),
        Consumer(broker, num_thread=3, id=3, thread_process=pool_thread),
        Consumer(broker, num_thread=4, id=4, thread_process=pool_thread),
        Consumer(broker, num_thread=5, id=5, thread_process=pool_thread),
    ]

    threads = [consumer.pull("message", consumer_callback) for consumer in consumers]
    return threads


def producer_callback(cb_producer: Producer):
    sleep = 0
    iterator = 0
    start = time.time()
    while iterator <= 100000:
        cb_producer.push("message", value=f"Test {iterator} 1")
        iterator += 1
        time.sleep(sleep)
    end = time.time()
    print("Duration", (end-start))


def producer_thread(pool_thread: Executor):
    future = pool_thread.submit(producer_callback, producer)
    return [future]


with ThreadPoolExecutor() as thread_pool:
    all_threads = list(chain(consumer_thread(thread_pool), producer_thread(thread_pool)))
    for _thread in all_threads:
        _thread.result()
