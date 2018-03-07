from celery.backends.database.models import Task as CeleryTask
import networkx as nx
from networkx.algorithms.dag import is_directed_acyclic_graph
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_json import MutableJson

Base = declarative_base()

class Workflow(Base):
    __tablename__ = 'workflow'
    id = Column(Integer, primary_key=True)
    dag_adjacency_list = Column(MutableJson)

    @property
    def execution_graph(self):
        d = self.dag_adjacency_list
        G = nx.DiGraph()

        for node in d.keys():
            nodes = d[node]
            if len(nodes) == 0:
                G.add_node(int(node))
                continue
            G.add_edges_from([(int(node), n) for n in nodes])
        return G


class Task(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True)
    celery_task_id = Column(Integer, ForeignKey(CeleryTask.id))
    celery_task = relationship(CeleryTask)
    sleep = Column(Integer)
