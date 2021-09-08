import threading
import time

from smart_load_balancer.work import Work
from smart_load_balancer.worker import Worker


def test_worker_init():
    def add_func(wrk):
        pass

    l = threading.Lock()
    wrk = Worker(worker_id=10, work_mutex=l, add_work_func=add_func)
    assert wrk.id == 10
    assert wrk.working == False


def test_worker_start():
    count = 0

    def work_test_func(name, data, wrk_data):
        nonlocal count
        count += 1
        time.sleep(0.2)
        print("%s " % name)
        return name

    wrk = Work(name="olia", data=None, work_func=work_test_func)
    wrk2 = Work(name="olia2", data=None, work_func=work_test_func)

    def add_func(w):
        if count < 2:
            w.works_queue.put(wrk2)

    l = threading.Lock()
    w = Worker(worker_id=10, work_mutex=l, add_work_func=add_func)
    with l:
        w.works_queue.put(wrk)
    w.start()
    time.sleep(0.1)
    assert w.working == True
    assert w.name == "olia"
    res = wrk.wait()
    assert res.res == "olia"
    res = wrk2.wait()
    assert w.working == False
    assert count == 2
    assert res.res == "olia2"
    assert w.name == "olia2"
    assert res.err is None


def test_worker_return_error():
    err = Exception('test')

    def work_test_func(name, data, wrk_data):
        raise err

    wrk = Work(name="olia", data=None, work_func=work_test_func)

    def add_func(w):
        pass

    l = threading.Lock()
    w = Worker(worker_id=10, work_mutex=l, add_work_func=add_func)
    with l:
        w.works_queue.put(wrk)
    w.start()
    res = wrk.wait()
    assert res.res is None
    assert res.err == err


def test_worker_keeps_data():
    err = Exception('test')

    def work_test_func(name, data, wrk_data):
        wrk_data["name"] = "olia"
        wrk_data["model"] = "model"

    wrk = Work(name="olia", data=None, work_func=work_test_func)

    def add_func(w):
        pass

    l = threading.Lock()
    w = Worker(worker_id=10, work_mutex=l, add_work_func=add_func)
    with l:
        w.works_queue.put(wrk)
    w.start()
    res = wrk.wait()
    assert w.data["name"] == "olia"
    assert w.data["model"] == "model"

    def work_test_func2(name, data, wrk_data):
        assert wrk_data["name"] == "olia"
        assert wrk_data["model"] == "model"

    wrk = Work(name="olia", data=None, work_func=work_test_func2)
    with l:
        w.works_queue.put(wrk)
    w.start()
    res = wrk.wait()
    assert res.err is None
