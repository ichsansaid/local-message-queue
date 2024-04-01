from balancer.balancer_base import BalancerBase


class HashBasedBalancer(BalancerBase):
    def next(self, value):
        self.mutex.acquire()
        if value is not None:
            if isinstance(value, int):
                raw_num = value
            else:
                raw_num = hash(str(value))
            next_iter = raw_num % len(self.data)
        else:
            next_iter = (self.iterator + 1) % len(self.data)
        self.iterator = next_iter
        self.mutex.release()
        return self.data[next_iter]

