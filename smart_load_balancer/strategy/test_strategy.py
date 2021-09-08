from smart_load_balancer.balancer import Balancer, add_work_dic
from smart_load_balancer.strategy.strategy import Oldest, GroupsByNameWithTime
from smart_load_balancer.work import Work
from smart_load_balancer.worker import Worker


def test_oldest(caplog):
    works = dict()
    add_work_dic(works, Work(name="olia", added=20))
    add_work_dic(works, Work(name="olia1", data="w1", added=10))
    wrk = Oldest().get_work(None, works, None)
    bal = Balancer(wrk_count=3)
    assert wrk.data == "w1"


def test_group_oldest(caplog):
    works = dict()
    add_work_dic(works, Work(name="olia", added=20))
    add_work_dic(works, Work(name="olia1", data="w1", added=10))
    wrk = GroupsByNameWithTime().get_work(Worker(0), works, [Worker(0)])
    assert wrk.data == "w1"


def test_group_by_name(caplog):
    works = dict()
    add_work_dic(works, Work(name="olia", data="w1", added=20))
    add_work_dic(works, Work(name="olia1", data="w2", added=21))
    add_work_dic(works, Work(name="olia2", data="w3", added=22))
    worker = Worker(0)
    worker.name = "olia2"
    wrk = GroupsByNameWithTime().get_work(worker, works, [Worker(0)])
    assert wrk.data == "w3"
    worker.name = "olia"
    wrk = GroupsByNameWithTime().get_work(worker, works, [Worker(0)])
    assert wrk.data == "w1"


def test_group_by_name_time(caplog):
    works = dict()
    add_work_dic(works, Work(name="olia", data="w1", added=9))
    add_work_dic(works, Work(name="olia1", data="w2", added=21))
    add_work_dic(works, Work(name="olia2", data="w3", added=22))
    worker = Worker(0)
    worker.name = "olia2"
    wrk = GroupsByNameWithTime().get_work(worker, works, [Worker(0)])
    assert wrk.data == "w1"
    wrk = GroupsByNameWithTime(name_penalty=20).get_work(worker, works, [Worker(0)])
    assert wrk.data == "w3"


def test_group_by_name_other_worker(caplog):
    works = dict()
    add_work_dic(works, Work(name="olia", data="w1", added=21))
    add_work_dic(works, Work(name="olia1", data="w2", added=21))
    worker = Worker(0)
    worker2 = Worker(1)
    worker2.name = "olia"
    wrk = GroupsByNameWithTime().get_work(worker, works, [])
    assert wrk.data == "w1"
    wrk = GroupsByNameWithTime().get_work(worker, works, [worker, worker2])
    assert wrk.data == "w2"
