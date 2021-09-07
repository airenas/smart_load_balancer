import logging
import queue
import threading


class Worker:
    def __init__(self, worker_id, work_mutex=None, add_work_func=None):
        logging.info("Init worker %d" % worker_id)
        self.id = worker_id
        self.working = False
        self.working_mutex = work_mutex
        self.add_work_func = add_work_func
        self.works_queue = queue.Queue(maxsize=10)
        self.name = None

    def start(self):
        threading.Thread(target=self.work_func, daemon=True).start()
        logging.info("Started worker %d" % self.id)

    def waiting(self):
        return not self.working and self.works_queue.empty()

    def work_func(self):
        while True:
            wrk = self.works_queue.get()
            logging.info("Worker %d got work %s" % (self.id, wrk.name))
            try:
                with self.working_mutex:
                    self.name = wrk.name
                    self.working = True
                wrk.res = wrk.work(self.id)
                logging.info("Worker %d completed %s" % (self.id, wrk.name))
            except Exception as err:
                logging.error("Worker %d failed completing %s" % (self.id, wrk.name))
                logging.error(err)
                wrk.err = err
            finally:
                wrk.done()
                with self.working_mutex:
                    self.working = False
                    self.add_work_func(self)
