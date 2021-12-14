import sys
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple

from smart_load_balancer.work import Work
from smart_load_balancer.worker import Worker, WorkerInfo


class Strategy(ABC):
    @abstractmethod
    def get_work(self, worker: WorkerInfo, works: Dict[str, List[Tuple[float, Work]]], workers: List[Worker]) -> Work:
        pass


class Oldest(Strategy):
    def __init__(self):
        pass

    def get_work(self, worker, works: Dict[str, List[Tuple[float, Work]]], workers) -> Work:
        best_v = sys.maxsize
        best_wrk = None
        for key in works:
            _, wrk = works[key][0]
            v = wrk.added + wrk.priority
            if v < best_v:
                best_v = v
                best_wrk = wrk
        return best_wrk


class GroupsByNameWithTime(Strategy):
    def __init__(self, name_penalty=10, other_workers_exist_penalty=1):
        self._name_penalty = name_penalty
        self._other_workers_exist_penalty = other_workers_exist_penalty
        pass

    def get_work(self, worker, works: Dict[str, List[Tuple[float, Work]]], workers) -> Work:
        t = time.time()
        best_v = sys.maxsize
        best_wrk = None
        for key in works:
            _, wrk = works[key][0]
            v = wrk.added - t + wrk.priority
            if wrk.name != worker.name:
                v = v + self._name_penalty
                if _has_other(workers, wrk.name, worker):
                    v = v + self._other_workers_exist_penalty
            if v < best_v:
                best_v = v
                best_wrk = wrk
        return best_wrk


class GroupsByNameWithTimeNoSameWorker(Strategy):
    """
    Strategy does not pass a work to an empty worker if there is one with the same name
    """

    def __init__(self, name_penalty=10):
        self._name_penalty = name_penalty
        pass

    def get_work(self, worker, works: Dict[str, List[Tuple[float, Work]]], workers) -> Work:
        t = time.time()
        best_v = sys.maxsize
        best_wrk = None
        for key in works:
            _, wrk = works[key][0]
            v = wrk.added - t + wrk.priority
            if wrk.name != worker.name:
                v = v + self._name_penalty
                if _has_other(workers, wrk.name, worker):
                    continue
            if v < best_v:
                best_v = v
                best_wrk = wrk
        return best_wrk


def _has_other(workers: List[WorkerInfo], name: str, worker: WorkerInfo) -> bool:
    for w in workers:
        if w.name == name and w != worker:
            return True
    return False
