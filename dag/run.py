from .models import Workflow, Task, CeleryTask, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .conf import DATABASE_URI, QUEUE_NAME
from .task import run
import random

engine = create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.drop_all(engine)
CeleryTask.__table__.drop(engine)
CeleryTask.__table__.create(engine, checkfirst=True)
Base.metadata.create_all(engine)

for i in range(8):
    session.add(Task(sleep=random.randint(1, 7))) # sleep for 1-7 secs

session.add(
    Workflow(
        dag_adjacency_list = dict([
            (1, [3]),
            (2, [4]),
            (3, [5]),
            (4, [5]),
            (5, [6, 7]),
            (6, [8]),
            (7, [8])
        ])
    )
)

session.commit()

workflow = session.query(Workflow).first()

run.apply_async(
    args=(workflow.id,),
    queue=QUEUE_NAME
)
