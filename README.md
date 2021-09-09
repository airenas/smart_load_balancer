# Smart load balancer

![Python](https://github.com/airenas/smart_load_balancer/workflows/Python/badge.svg) [![Coverage Status](https://coveralls.io/repos/github/airenas/smart_load_balancer/badge.svg?branch=main)](https://coveralls.io/github/airenas/smart_load_balancer?branch=main) ![CodeQL](https://github.com/airenas/smart_load_balancer/workflows/CodeQL/badge.svg)

Python package to load balance stateful workers.

## Objective

Imagine a stateful service. It loads a model into GPU and keeps it in a GPU memory for faster processing. Lets say a load time of a model is `5s` and a job process time is `0.01s`. There can be several models, but only one fits into a memory at the time. A job request contains a required model as a parameters. When a job arrives then the service compares a loaded model with a requested one, if they are the same then it processes the job else it loads the new model and processes the job.

Example: a lot of jobs arrive at the same time. Models of the jobs are: `m1, m2, m1, m2, m1, m2`. Let's ignore the time of the job processing which is `0.01s`. If we process the jobs in the order of their arrival then it will take `5s(load m1) + 5s(load m2) + ... = 30s.`. But if we will do `m1, m1, m1, m2, m2, m2` then it will take `10s`.  

This is the problem the package tries to solve. It registers jobs by names, when a worker completes a job and want a new one, then the code provides the best job trying to avoid switching of a state for a worker.    

## Installation
```bash
pip install git+git://github.com/airenas/smart_load_balancer.git@v0.1.17#egg=smart_load_balancer
```

## Usage

### Start the balancer

```python
from smart_load_balancer.balancer import Balancer

balancer = Balancer(wrk_count=1)
balancer.start()
``` 

### Add a job

```python
from smart_load_balancer.balancer import Work

work = Work(name=model_name, data=some_data, work_func=work_process_function)
balancer.add_wrk(work)
w_res = work.wait()    # waits until a job is processed
if w_res.err is not None:
    raise w_res.err
print(w_res.res)  # use the result
```

The sample of the `work_process_function` used above:

```python
def work_process_function(model_name, some_data, workers_data):
    # it is a synchronized function - no race here if you don't use any other global object
    # workers_data: dict - is a state of a worker
    previous_model_name = workers_data.get("name")
    model = workers_data.get("model")
    if previous_model_name != model_name or model is None:
        # make sure we have correct worker in case new model load fails
        workers_data["name"] = ""
        workers_data["model"] = None
        
        workers_data["model"] = some_load_function(model_name)
        
        workers_data["name"] = model_name
    
    return model.process(some_data)
```

### Work selection strategies

There is only two strategies implemented at the time that meet my needs. `strategy.Oldest` - it processes jobs in the order of the arrival. `strategy.GroupsByNameWithTime` - selects the jobs with the current state of the worker, but switches to the other model if there is a job waiting for more than `10s`.

You can implement a new strategy. See the implemented ones as a sample: [smart_load_balancer/strategy/strategy.py](smart_load_balancer/strategy/strategy.py). To use a new strategy initiate the `balancer` with: 

```python
balancer = Balancer(wrk_count=1, strategy=MyNewStrategy())
```
`Strategy` class must implement one method `get_work(worker: Worker, works: Dict[str, Deque[Work]], workers: List[Worker]) -> Work`. `worker` - is the worker wanting for a job, `works` - a dict of all available jobs, `workers` - a list of all workers including the `worker`. You must select the best work from `works` and return it or None if there is no job for the `worker`.

---

## License

Copyright © 2021, [Airenas Vaičiūnas](https://github.com/airenas).

Released under the [The 3-Clause BSD License](LICENSE).

---