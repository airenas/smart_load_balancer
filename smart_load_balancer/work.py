import logging
import queue
import time


class Work:
    def __init__(self, name, data=None, work_func=None, added=time.time()):
        logging.info("Init work at %d" % added)
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

    def work(self, worker_id):
        self.worker_id = worker_id
        return self.work_func(self.name, self.data)
