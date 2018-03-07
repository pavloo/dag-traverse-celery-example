#!/usr/bin/env python
from celery import Celery
from celery.signals import task_postrun, task_prerun, after_setup_logger, task_failure
from celery.task.control import revoke
from config import RABBIT_HOST, RABBIT_USER, RABBIT_PSWD, SQLALCHEMY_DATABASE_URI, logging_handler
from job_status_service.models import Task, Job, Pipeline
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from job_runner.runners import RunnerFactory, send_result_to_rabbitmq, RESULT_QUEUE_NAME
from job_runner.exceptions import PipelineRunError
from job_status_service.utils.helpers import find_graph_entrance
from celery.result import AsyncResult
from celery.states import SUCCESS
from celery.backends.database.models import Task as CeleryTask
import logging
import json


database_uri = SQLALCHEMY_DATABASE_URI
app = Celery('houston', backend='db+' + database_uri,
             broker='amqp://{0}:{1}@{2}'.format(RABBIT_USER, RABBIT_PSWD, RABBIT_HOST))

engine = create_engine(database_uri)
Session = sessionmaker(bind=engine)
session = None
task_runner = None


@after_setup_logger.connect
def on_celery_setup_logging(**kwargs):
    if isinstance(logging_handler, logging.StreamHandler):
        return
    logger = kwargs['logger']
    logger.addHandler(logging_handler)


@task_prerun.connect
def prerun(*args, **kwargs):
    global session
    session = Session()


@task_postrun.connect
def postrun(*args, **kwargs):
    session.flush()
    session.close()


@task_failure.connect
def handle_task_fail(task_id, *args, **kwargs):
    session = Session()
    task = session.query(Task).filter(Task.celery_task_id == task_id).first()

    d = {}
    d['status'] = 'error'
    d['task_id'] = task.id
    d['pipeline_id'] = task.pipeline.id

    try:
        send_result_to_rabbitmq(d, RESULT_QUEUE_NAME)
    except Exception as e:
        print('Error sending result to rabbitmq: {}'.format(e))

    finally:
        session.close()


@app.task(bind=True, default_retry_delay=60, max_retries=5)
def run(self, *args, **kwargs):
    run_task(self, *args, **kwargs)


def _is_ready_to_be_processed(task, graph):
    tasks = session.query(Task).filter(Task.id.in_(list(graph.predecessors(task.id)))).all()
    for dep_task in tasks:
        if not dep_task.celery_task_id or \
           not AsyncResult(dep_task.celery_task_id).state == SUCCESS:
            return False
    return True


def run_task(context, pipeline_id, current_task_id=None):
    pipeline = session.query(Pipeline).filter_by(id=pipeline_id).one()
    if current_task_id and current_task_id not in pipeline.execution_graph.nodes:
        raise PipelineRunError('Task with id {} is not part of Pipeline with id {}'.format(
            current_task_id,
            pipeline_id
        ))
    graph = pipeline.execution_graph

    next_task_ids = []
    if current_task_id:
        task = session.query(Task).get(current_task_id)
        if not _is_ready_to_be_processed(task, graph):
            return
        task_runner = RunnerFactory(
            task,
            context.request.id,
            session
        ).build()
        task_runner.run()
        next_task_ids = list(graph.successors(current_task_id))
        if len(next_task_ids) == 0:
            task_runner.clean_up_environment()
    else:
        next_task_ids = find_graph_entrance(graph)

    # Manually set task's state to SUCCESS because when
    # we get to the next task the task status may not be updated yet
    # to SUCCESS and the pipeline may just stuck
    context.update_state(state=SUCCESS)

    for task_id in next_task_ids:
        run.apply_async(
            args=(pipeline_id, task_id,),
            queue=pipeline.whitelist_regions[0]
        )
