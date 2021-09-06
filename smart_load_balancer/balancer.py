import logging
import queue
import threading
import time
from collections import deque

from worker import Worker


def has_other(workers, name):
    for w in workers:
        if w.name == name:
            return True
    return False


class Balancer:
    def __init__(self, wrk_count=1):
        logging.info("Init balancer with %d workers" % wrk_count)
        self.works_count = wrk_count
        self.works_queue = queue.Queue()
        self.works_lock = threading.Lock()
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
        for wrk in self.works_queue:
            name = wrk.name
            with self.works_lock:
                q = self.works_map.get(name)
                if q is None:
                    q = deque()
                    self.works_map[name] = q
                q.append(wrk)
                w = self.get_best_free_worker(wrk.name)
                if w is not None:
                    self.try_add_work(w)

    def try_add_work(self, w):
        t = time.time()
        best_v = 0.0
        best_wrk = None
        for key in self.works_map:
            wrk = self.works_map[key][0]
            v = t - wrk.added
            if wrk.name == w.name:
                v = v - 10
            elif has_other(self.workers, wrk.name):
                v = v + 1
            if v > best_v:
                best_v = v
                best_wrk = wrk
        if best_wrk is not None:
            wrk = self.works_map[wrk.name].popleft()
            if not self.works_map[wrk.name]:
                del self.works_map[wrk.name]
            w.works_queue.queue(best_wrk)

    def get_best_free_worker(self, name):
        b = None
        for w in self.workers:
            if w.waiting():
                if w.name == name:
                    return w
                if b is None or w.name is None:  # empty worker
                    b = w
        return b
