import threading
from abc import abstractmethod, ABC
from typing import List


class BalancerBase[T](ABC):
    def __init__(self, data: List[T]):
        self.data = data
        self.mutex = threading.Lock()
        self.iterator = 0

    @abstractmethod
    def next(self, value) -> T:
        pass
