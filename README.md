### About
This project is an example project that complements a [blog post]() about how to implement a job scheduler for traversing DAG of tasks in Python's [Celery](http://celery.readthedocs.io)

### How to run

#### Dependencies
1. `MySQL 5.6`+
2. `RabbitMQ`
3. `Python 3`

#### Steps
1. `git clone git@github.com:pavloo/dag-traverse-celery-example.git`
2. `cd dag-traverse-celery-example`
3. `pip install -r requirements.txt`
4. `celery worker -A dag.task -Q queue-1`
5. `python -m dag.run`
