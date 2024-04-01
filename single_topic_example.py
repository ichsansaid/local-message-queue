import time
from concurrent import futures

from consumer import Consumer
from single_centered_topic_queue import SingleCenteredTopicMessageQueue


def consumer_callback(consumer: Consumer, value, key):
    # Long task
    print(f"Start Task of value ({value}) and key ({key}) at {consumer.id}")
    time.sleep(1)


with futures.ThreadPoolExecutor() as thread:
    msg = SingleCenteredTopicMessageQueue(
        topic="any",
        num_threads=2,
        thread_pool=thread,
        callback=consumer_callback
    )
    tasks = msg.start()
    iterator = 0
    while iterator <= 1000:
        msg.push(value=f"Test {iterator} 1")
        iterator += 1
    for task in tasks:
        task.result()
