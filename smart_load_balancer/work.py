import logging
import queue
import time
from typing import Any, Callable, Dict

from smart_load_balancer.worker import WorkerInfo

logger = logging.getLogger(__name__)


class Work:
    def __init__(self, name: str, data: Any = None, work_func: Callable[[str, Any, Dict], Any] = None,
                 added=time.time()):
        logger.info("Init work '%s' at %d" % (name, added))
        self.added = added
        self.name = name
        self.work_func = work_func
        self.data = data
        self.err = None
        self.res = None
        self.worker_id = None
        self.wait_queue = queue.Queue(maxsize=1)

    def done(self):
        return self.wait_queue.put(self, block=False)

    def wait(self):
        return self.wait_queue.get()

    def work(self, worker: WorkerInfo) -> Any:
        self.worker_id = worker.id
        return self.work_func(self.name, self.data, worker.data)
