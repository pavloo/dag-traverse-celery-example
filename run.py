from .models import Workflow, Task, CeleryTask, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://root:@localhost/dag_celery?charset=utf8mb4')

Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.drop_all(engine)
CeleryTask.__table__.drop(engine)
CeleryTask.__table__.create(engine, checkfirst=True)
Base.metadata.create_all(engine)

for _ in range(4):
    session.add(Task())

session.add(
    Workflow(
        dag_adjacency_list = dict([
            (1, [2]),
            (2, [3])
        ])
    )
)

session.commit()

session.query(Workflow).all()[0].execution_graph.nodes
