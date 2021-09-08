import sys
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Deque

from smart_load_balancer.work import Work
from smart_load_balancer.worker import Worker


def has_other(workers, name):
    for w in workers:
        if w.name == name:
            return True
    return False


class Strategy(ABC):
    @abstractmethod
    def get_work(self, worker: Worker, works: Dict[str, Deque[Work]], workers: List[Worker]) -> Work:
        pass


class Oldest(Strategy):
    def __init__(self):
        pass

    def get_work(self, worker, works, workers):
        best_v = sys.maxsize
        best_wrk = None
        for key in works:
            wrk = works[key][0]
            v = wrk.added
            if v < best_v:
                best_v = v
                best_wrk = wrk
        return best_wrk


class GroupsByNameWithTime(Strategy):
    def __init__(self, name_penalty=10, other_workers_exist_penalty=1):
        self.name_penalty = name_penalty
        self.other_workers_exist_penalty = other_workers_exist_penalty
        pass

    def get_work(self, worker, works, workers):
        t = time.time()
        best_v = sys.maxsize
        best_wrk = None
        for key in works:
            wrk = works[key][0]
            v = wrk.added - t
            if wrk.name != worker.name:
                v = v + self.name_penalty
                if has_other(workers, wrk.name):
                    v = v + self.other_workers_exist_penalty
            if v < best_v:
                best_v = v
                best_wrk = wrk
        return best_wrk
