import logging
import time

from smart_load_balancer.balancer import Balancer
from smart_load_balancer.work import Work


def test_balancer_init(caplog):
    # caplog.set_level(logging.INFO)
    bal = Balancer(wrk_count=1)
    assert len(bal._Balancer__workers) == 1

    bal = Balancer(wrk_count=3)
    assert len(bal._Balancer__workers) == 3


def test_balancer_add_wrk():
    bal = Balancer(wrk_count=1)
    assert len(bal._Balancer__workers) == 1
    bal.start()

    def work_test_func(name, data, wrk_data):
        return name

    wrk = Work(name="olia", data=None, work_func=work_test_func)
    bal.add_wrk(wrk)
    res = wrk.wait()
    assert res.res == "olia"


def test_balancer_add_several_wrk():
    bal = Balancer(wrk_count=1)
    bal.start()

    wc = 0

    def work_test_func(name, data, wrk_data):
        nonlocal wc
        wc += 1
        time.sleep(0.1)
        return wc

    wrk = Work(name="olia", data="w1", work_func=work_test_func)
    wrk2 = Work(name="olia1", data="w2", work_func=work_test_func)
    wrk3 = Work(name="olia", data="w3", work_func=work_test_func)
    wrk4 = Work(name="olia", data="w4", work_func=work_test_func)
    bal.add_wrk(wrk)
    bal.add_wrk(wrk2)
    bal.add_wrk(wrk3)
    bal.add_wrk(wrk4)
    res4 = wrk4.wait()
    res3 = wrk3.wait()
    res = wrk.wait()
    res2 = wrk2.wait()
    assert res.res == 1
    assert res2.res == 4
    assert res3.res == 2
    assert res4.res == 3


def test_balancer_several_workers(caplog):
    caplog.set_level(logging.INFO)
    bal = Balancer(wrk_count=2)
    bal.start()

    def work_test_func(name, data, wrk_data):
        time.sleep(0.1)
        return name

    wrk = Work(name="olia", data="w1", work_func=work_test_func)
    wrk2 = Work(name="olia1", data="w2", work_func=work_test_func)
    wrk3 = Work(name="olia", data="w3", work_func=work_test_func)
    wrk4 = Work(name="olia1", data="w4", work_func=work_test_func)
    bal.add_wrk(wrk)
    bal.add_wrk(wrk2)
    bal.add_wrk(wrk3)
    bal.add_wrk(wrk4)
    res4 = wrk4.wait()
    res3 = wrk3.wait()
    res = wrk.wait()
    res2 = wrk2.wait()
    assert res.worker_id == 0
    assert res2.worker_id == 1
    assert res3.worker_id == 0
    assert res4.worker_id == 1


def test_balancer_prefer_old():
    bal = Balancer(wrk_count=1)
    bal.start()

    wc = 0

    def work_test_func(name, data, wrk_data):
        nonlocal wc
        wc += 1
        time.sleep(0.1)
        return wc

    t = time.time()
    wrk = Work(name="olia", data="w1", work_func=work_test_func, added=t)
    wrk2 = Work(name="olia1", data="w2", work_func=work_test_func, added=t - 11)
    wrk3 = Work(name="olia", data="w3", work_func=work_test_func, added=t + 1)
    wrk4 = Work(name="olia", data="w4", work_func=work_test_func, added=t + 2)
    bal.add_wrk(wrk)
    bal.add_wrk(wrk3)
    bal.add_wrk(wrk4)
    bal.add_wrk(wrk2)
    res4 = wrk4.wait()
    res3 = wrk3.wait()
    res = wrk.wait()
    res2 = wrk2.wait()
    assert res.res == 1
    assert res2.res == 2
    assert res3.res == 3
    assert res4.res == 4


def test_balancer_prefer_empty(caplog):
    caplog.set_level(logging.INFO)
    bal = Balancer(wrk_count=2)
    bal._Balancer__workers[0].name = "olia"
    bal.start()

    def work_test_func(name, data, wrk_data):
        return name

    t = time.time()
    wrk = Work(name="olia1", data="w1", work_func=work_test_func, added=t)
    bal.add_wrk(wrk)
    res = wrk.wait()
    assert res.worker_id == 1
