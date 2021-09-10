import logging
import queue
import threading
from abc import ABC, abstractmethod
from typing import Callable, Any

logger = logging.getLogger(__name__)


class WorkerInfo(ABC):
    def __init__(self):
        self.data = dict()
        self.name = None
        self.id = 0
        self.wrk_done = 0
        self.wrk_switch = 0
        self.wrk_done_last = 0  # last wrk done after switch
        self.working = False  # managed from balancer
        pass

    @abstractmethod
    def add_work(self, wrk):
        pass


class Worker(WorkerInfo):
    def __init__(self, worker_id: int, work_mutex=threading.Lock, finish_work_func: Callable[[WorkerInfo], Any] = None):
        super(Worker, self).__init__()
        logger.info("Init worker %d" % worker_id)
        self.id = worker_id
        self._working_mutex = work_mutex
        self.finish_work_func = finish_work_func
        self._works_queue = queue.Queue(maxsize=1)

    def start(self):
        threading.Thread(target=self.work_func, daemon=True).start()
        logger.info("Started worker %d" % self.id)

    def waiting(self):
        return not self.working

    def work_func(self):
        while True:
            wrk = self._works_queue.get()
            if self.name != wrk.name:
                self.wrk_switch += 1
                self.wrk_done_last = 0
                logger.warning("Worker %d switch from '%s' to '%s'" % (self.id, self.name, wrk.name))
            try:
                with self._working_mutex:
                    self.name = wrk.name
                wrk.res = wrk.work(self)
                logger.info("Worker %d completed %s" % (self.id, wrk.name))
            except Exception as err:
                logger.error("Worker %d failed completing %s" % (self.id, wrk.name))
                logger.error(err)
                wrk.err = err
            finally:
                wrk.done()
                self.wrk_done += 1
                self.wrk_done_last += 1
                with self._working_mutex:
                    self.finish_work_func(self)

    def add_work(self, wrk):
        logger.info("Worker %d got work %s" % (self.id, wrk.name))
        logger.info("Worker %d info (all:%d, switch:%d, last:%d)" % (
            self.id, self.wrk_done, self.wrk_switch, self.wrk_done_last))
        self.working = True
        self._works_queue.put(wrk)
