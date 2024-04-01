from balancer.balancer_base import BalancerBase


class RoundRobinBalancer(BalancerBase):

    def next(self, _value):
        self.mutex.acquire()
        next_iter = self.iterator % len(self.data)
        self.iterator += 1
        self.mutex.release()
        return self.data[next_iter]
