import heapq
import logging
import queue
import threading
from queue import Queue
from typing import List, Dict, Tuple

from smart_load_balancer.strategy.strategy import GroupsByNameWithTime, Strategy
from smart_load_balancer.work import Work
from smart_load_balancer.worker import Worker, WorkerInfo

logger = logging.getLogger(__name__)


class Balancer:
    def __init__(self, wrk_count=1, strategy: Strategy = GroupsByNameWithTime()):
        logger.info("Init balancer with %d workers" % wrk_count)
        self.works_count = wrk_count
        self.__works_queue: Queue[Work] = queue.Queue(maxsize=500)
        self.__works_lock = threading.Lock()
        self.waiting = 0
        self.strategy = strategy
        with self.__works_lock:
            self.__works_map: Dict[str, List[Tuple[float, Work]]] = dict()
            self.__workers = list()
            for i in range(wrk_count):
                self.__workers.append(
                    Worker(worker_id=i, work_mutex=self.__works_lock, finish_work_func=self.__finish_and_add_work))

    def start(self):
        threading.Thread(target=self.__queue_func, daemon=True).start()
        for w in self.__workers:
            w.start()

    def add_wrk(self, wrk):
        return self.__works_queue.put(wrk)

    def __queue_func(self):
        while True:
            wrk = self.__works_queue.get()
            name = wrk.name
            logger.info("Register work for %s" % wrk.name)
            with self.__works_lock:
                add_work_dic(self.__works_map, wrk)
                self.waiting += 1
                logger.info("Register work for %s - %d in line" % (wrk.name, len(self.__works_map[name])))
                w = self.__get_best_free_worker(name)
                if w is not None:
                    self.__finish_and_add_work(w)

    def __finish_and_add_work(self, worker: WorkerInfo):
        """
        Tries add a new work into workers queue
        No race condition here
        """
        worker.working = False
        logger.info("Try add work - works waiting %d" % self.waiting)
        best_wrk = self.strategy.get_work(worker, self.__works_map, self.__workers)
        if best_wrk is not None:
            logger.info("Best work selected %s" % best_wrk.name)
            wrk = pop_work_dic(self.__works_map, best_wrk.name)
            self.waiting -= 1
            logger.info("Pass work %s(%.d) to worker %d" % (wrk.name, wrk.priority, worker.id))
            worker.add_work(wrk)

    def __get_best_free_worker(self, name):
        b = None
        for w in self.__workers:
            if w.waiting():
                if w.name == name:
                    return w
                if b is None:  # empty worker
                    b = w
                if b.name and w.name is None:  # other available worker
                    b = w
        return b


def add_work_dic(works: Dict[str, List[Tuple[float, Work]]], wrk: Work):
    name = wrk.name
    wl = works.get(name)
    if wl is None:
        wl = list()
        works[name] = wl
    p = wrk.added + wrk.priority
    heapq.heappush(wl, (p, wrk))


def pop_work_dic(works: Dict[str, List[Tuple[float, Work]]], name: str) -> Work:
    wl = works.get(name)
    _, wrk = heapq.heappop(wl)
    if not works[name]:
        del works[name]
    return wrk
