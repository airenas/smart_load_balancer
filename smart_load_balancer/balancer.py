import logging
import queue
import threading
import time
from collections import deque

from smart_load_balancer.worker import Worker


def has_other(workers, name):
    for w in workers:
        if w.name == name:
            return True
    return False


class Balancer:
    def __init__(self, wrk_count=1):
        logging.info("Init balancer with %d workers" % wrk_count)
        self.works_count = wrk_count
        self.works_queue = queue.Queue(maxsize=500)
        self.works_lock = threading.Lock()
        self.waiting = 0
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
            logging.info("Register work for %s" % wrk.name)
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
        t = time.time()
        best_v = 10000000000.0
        best_wrk = None
        logging.info("Try add work - works waiting %d" % self.waiting)
        for key in self.works_map:
            wrk = self.works_map[key][0]
            v = wrk.added - t
            if wrk.name == w.name:
                v = v - 10
            elif has_other(self.workers, wrk.name):
                v = v + 1
            logging.info("Value %f for %s" % (v, wrk.name))
            if v < best_v:
                best_v = v
                best_wrk = wrk
        if best_wrk is not None:
            logging.info("Best value %f for %s" % (best_v, best_wrk.name))
            wrk = self.works_map[best_wrk.name].popleft()
            self.waiting -= 1
            if not self.works_map[wrk.name]:
                del self.works_map[wrk.name]
            logging.info("Pass work %s to worker %d" % (wrk.name, w.id))
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
