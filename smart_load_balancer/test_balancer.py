import logging
import time
from typing import Tuple, List, Dict

from smart_load_balancer.balancer import Balancer, add_work_dic, pop_work_dic
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


def test_add_wrk(caplog):
    caplog.set_level(logging.INFO)
    wrks: Dict[str, List[Tuple[float, Work]]] = dict()
    add_work_dic(wrks, Work(name="olia", data=1, added=10, priority=1))
    add_work_dic(wrks, Work(name="olia", data=2, added=10, priority=2))
    add_work_dic(wrks, Work(name="olia", data=3, added=10, priority=3))
    add_work_dic(wrks, Work(name="olia1", data=4, added=10, priority=1))
    assert len(wrks["olia"]) == 3
    assert wrks["olia"][0][0] == 11
    assert wrks["olia"][0][1].data == 1
    assert wrks["olia"][1][1].data == 2
    assert wrks["olia"][2][1].data == 3
    assert len(wrks["olia1"]) == 1
    assert wrks["olia1"][0][0] == 11
    assert wrks["olia1"][0][1].data == 4


def test_add_wrk_float(caplog):
    caplog.set_level(logging.INFO)
    wrks: Dict[str, List[Tuple[float, Work]]] = dict()
    add_work_dic(wrks, Work(name="olia", data=1, added=10.01, priority=1))
    add_work_dic(wrks, Work(name="olia", data=2, added=10.005, priority=1))
    assert len(wrks["olia"]) == 2
    assert wrks["olia"][0][0] == 11.005
    assert wrks["olia"][0][1].data == 2
    assert wrks["olia"][1][0] == 11.01
    assert wrks["olia"][1][1].data == 1


def test_add_pop_wrk_empty(caplog):
    caplog.set_level(logging.INFO)
    wrks: Dict[str, List[Tuple[float, Work]]] = dict()
    add_work_dic(wrks, Work(name="olia", data=1, added=10, priority=1))
    add_work_dic(wrks, Work(name="olia1", data=2, added=10, priority=2))
    assert len(wrks) == 2
    pop_work_dic(wrks, "olia")
    pop_work_dic(wrks, "olia1")
    assert len(wrks) == 0


def test_add_pop_wrk_float(caplog):
    caplog.set_level(logging.INFO)
    wrks: Dict[str, List[Tuple[float, Work]]] = dict()
    add_work_dic(wrks, Work(name="olia", data=1, added=0.001, priority=1))
    add_work_dic(wrks, Work(name="olia", data=2, added=0.0001, priority=1))
    add_work_dic(wrks, Work(name="olia", data=3, added=0.00011, priority=1))
    add_work_dic(wrks, Work(name="olia", data=4, added=0.000105, priority=1))
    wrk = pop_work_dic(wrks, "olia")
    assert wrk.data == 2
    wrk = pop_work_dic(wrks, "olia")
    assert wrk.data == 4


def test_add_pop_wrk(caplog):
    caplog.set_level(logging.INFO)
    wrks: Dict[str, List[Tuple[float, Work]]] = dict()
    add_work_dic(wrks, Work(name="olia", data=1, added=10, priority=1))
    add_work_dic(wrks, Work(name="olia", data=2, added=10, priority=0))
    add_work_dic(wrks, Work(name="olia", data=3, added=10, priority=-1))
    wrk = pop_work_dic(wrks, "olia")
    assert wrk.data == 3
    wrk = pop_work_dic(wrks, "olia")
    assert wrk.data == 2
    wrk = pop_work_dic(wrks, "olia")
    assert wrk.data == 1


def test_add_wrk_default_value(caplog):
    caplog.set_level(logging.INFO)
    wrks: Dict[str, List[Tuple[float, Work]]] = dict()
    for i in range(100):
        add_work_dic(wrks, Work(name="olia", data=i))
        time.sleep(0.0001)
    for i in range(100):
        wrk = pop_work_dic(wrks, "olia")
        assert wrk.data == i
