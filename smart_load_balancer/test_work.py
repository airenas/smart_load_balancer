import time

from smart_load_balancer.work import Work


def work_test_func(name, data):
    print("%s " % name)


def test_work_init():
    wrk = Work(name="olia", data=None, work_func=work_test_func)
    assert wrk.name == "olia"
    t = time.time()
    wrk = Work(name="olia", data=None, work_func=work_test_func, added=t)
    assert wrk.added == t


def test_work_start():
    done = False

    def work_func(name, data):
        nonlocal done
        done = True
        assert name == "olia"
        assert data == "data"

    wrk = Work(name="olia", data="data", work_func=work_func)
    wrk.work()
    assert wrk.name == "olia"
    assert done
