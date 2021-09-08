import logging
import queue
import threading
from collections import deque

from smart_load_balancer.strategy.strategy import GroupsByNameWithTime, Strategy
from smart_load_balancer.worker import Worker

logger = logging.getLogger(__name__)


class Balancer:
    def __init__(self, wrk_count=1, strategy: Strategy = GroupsByNameWithTime()):
        logger.info("Init balancer with %d workers" % wrk_count)
        self.works_count = wrk_count
        self.works_queue = queue.Queue(maxsize=500)
        self.works_lock = threading.Lock()
        self.waiting = 0
        self.strategy = strategy
        with self.works_lock:
            self.works_map = dict()
            self.workers = list()
            for i in range(wrk_count):
                self.workers.append(Worker(worker_id=i, work_mutex=self.works_lock, add_work_func=self.try_add_work))

    def start(self):
        threading.Thread(target=self.queue_func, daemon=True).start()
        for w in self.workers:
            w.start()

    def add_wrk(self, wrk):
        return self.works_queue.put(wrk, block=True)

    def queue_func(self):
        while True:
            wrk = self.works_queue.get()
            name = wrk.name
            logger.info("Register work for %s" % wrk.name)
            with self.works_lock:
                q = self.works_map.get(name)
                if q is None:
                    q = deque()
                    self.works_map[name] = q
                q.append(wrk)
                self.waiting += 1
                w = self.get_best_free_worker(name)
                if w is not None:
                    self.try_add_work(w)

    def try_add_work(self, w):
        logger.info("Try add work - works waiting %d" % self.waiting)
        best_wrk = self.strategy.get_work(w, self.works_map, self.workers)
        if best_wrk is not None:
            logger.info("Best work selected %s" % best_wrk.name)
            wrk = self.works_map[best_wrk.name].popleft()
            self.waiting -= 1
            if not self.works_map[wrk.name]:
                del self.works_map[wrk.name]
            logger.info("Pass work %s to worker %d" % (wrk.name, w.id))
            w.works_queue.put(wrk)

    def get_best_free_worker(self, name):
        b = None
        for w in self.workers:
            if w.waiting():
                if w.name == name:
                    return w
                if b is None:  # empty worker
                    b = w
                if b.name and w.name is None:  # other available worker
                    b = w
        return b


def add_work_dic(works, wrk):
    name = wrk.name
    q = works.get(name)
    if q is None:
        q = deque()
        works[name] = q
    q.append(wrk)
