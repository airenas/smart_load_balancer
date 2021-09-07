import sys
import time


def has_other(workers, name):
    for w in workers:
        if w.name == name:
            return True
    return False


class Oldest:
    def __init__(self):
        pass

    def get_work(self, worker, works, workers):
        best_v = sys.maxsize
        best_wrk = None
        for key in works:
            wrk = works[key][0]
            v = wrk.added
            if v < best_v:
                best_v = v
                best_wrk = wrk
        return best_wrk


class GroupsByNameWithTime:
    def __init__(self, name_penalty=10, other_workers_exist_penalty=1):
        self.name_penalty = name_penalty
        self.other_workers_exist_penalty = other_workers_exist_penalty
        pass

    def get_work(self, worker, works, workers):
        t = time.time()
        best_v = sys.maxsize
        best_wrk = None
        for key in works:
            wrk = works[key][0]
            v = wrk.added - t
            if wrk.name != worker.name:
                v = v + self.name_penalty
            elif has_other(workers, wrk.name):
                v = v + self.other_workers_exist_penalty
            if v < best_v:
                best_v = v
                best_wrk = wrk
        return best_wrk
