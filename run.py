from .models import Workflow, Task, CeleryTask, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .conf import DATABASE_URI, QUEUE_NAME
from .task import run

engine = create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.drop_all(engine)
CeleryTask.__table__.drop(engine)
CeleryTask.__table__.create(engine, checkfirst=True)
Base.metadata.create_all(engine)

for i in range(4):
    session.add(Task(sleep=(i + 1)))

session.add(
    Workflow(
        dag_adjacency_list = dict([
            (1, [2]),
            (2, [3])
        ])
    )
)

session.commit()

workflow = session.query(Workflow).first()

run.apply_async(
    args=(workflow.id,),
    queue=QUEUE_NAME
)
