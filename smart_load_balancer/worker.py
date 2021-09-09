import logging
import queue
import threading
from typing import Callable, Any

logger = logging.getLogger(__name__)


class WorkerInfo:
    def __init__(self):
        self.data = dict()
        self.name = None
        self.id = 0
        pass


class Worker(WorkerInfo):
    def __init__(self, worker_id: int, work_mutex=threading.Lock, add_work_func: Callable[[WorkerInfo], Any] = None):
        super(Worker, self).__init__()
        logger.info("Init worker %d" % worker_id)
        self.id = worker_id
        self.working = False
        self.working_mutex = work_mutex
        self.add_work_func = add_work_func
        self.works_queue = queue.Queue(maxsize=10)
        self.name = None

    def start(self):
        threading.Thread(target=self.work_func, daemon=True).start()
        logger.info("Started worker %d" % self.id)

    def waiting(self):
        return not self.working and self.works_queue.empty()

    def work_func(self):
        while True:
            wrk = self.works_queue.get()
            self.working = True   # do not use lock for boolean
            logger.info("Worker %d got work %s. Queue empty: %s" % (self.id, wrk.name, self.works_queue.empty()))
            try:
                with self.working_mutex:
                    self.name = wrk.name
                wrk.res = wrk.work(self)
                logger.info("Worker %d completed %s" % (self.id, wrk.name))
            except Exception as err:
                logger.error("Worker %d failed completing %s" % (self.id, wrk.name))
                logger.error(err)
                wrk.err = err
            finally:
                wrk.done()
                self.working = False
                with self.working_mutex:
                    if self.works_queue.empty():  # add only if no works pending
                        self.add_work_func(self)
