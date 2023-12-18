from logging import Logger
from typing import List, Union
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from ..engine.base import BaseMetadataStoreNode, BaseResourceNode



Base = declarative_base()

class Run(Base):
    __tablename__ = 'runs'
    id = Column(Integer, primary_key=True)
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime)

class Metric(Base):
    __tablename__ = 'metrics'
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer)
    key = Column(String)
    value = Column(Float)

class Param(Base):
    __tablename__ = 'params'
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer)
    key = Column(String)
    value = Column(Float)

class Tag(Base):
    __tablename__ = 'tags'
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer)
    key = Column(String)
    value = Column(String)

class Sample(Base):
    __tablename__ = 'samples'
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer)
    node_id = Column(Integer)
    location = Column(String)
    state = Column(String, default="new")
    end_time = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

class Node(Base):
    __tablename__ = 'nodes'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)
    init_time = Column(DateTime, default=datetime.utcnow)



class SqliteMetadataStore(BaseMetadataStoreNode):
    def __init__(self, name: str, uri: str, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, uri, loggers)
        self.session = None
    
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")

        # Create an engine that stores data in the local directory's sqlite.db file.
        engine = create_engine(f'{self.uri}', echo=True)

        # Create all tables in the engine (this is equivalent to "Create Table" statements in raw SQL).
        Base.metadata.create_all(engine)

        # Create a sessionmaker, binding it to the engine
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.log(f"Node '{self.name}' setup complete.")
    
    def get_run_id(self) -> int:
        run = self.session.query(Run).filter_by(end_time=None).first()
        return run.id
    
    def get_num_entries(self, resource_node: BaseResourceNode, state: str) -> int:
        node_id = self.session.query(Node).filter_by(name=resource_node.name).first().id
        return self.session.query(Sample).filter_by(node_id=node_id, state=state).count()
    
    def create_resource_tracker(self, resource_node: BaseResourceNode) -> None:
        resource_name = resource_node.name
        type_name = type(resource_node).__name__
        node = Node(name=resource_name, type=type_name)
        self.session.add(node)
        self.session.commit()

    def create_entry(self, resource_node: BaseResourceNode, filepath: str, state: str = "new", run_id: int = None) -> None:
        # in the future, refactor this by changing filepath to uri 
        node_id = self.session.query(Node).filter_by(name=resource_node.name).first().id
        sample = Sample(node_id=node_id, location=filepath, state=state, run_id=run_id)
        self.session.add(sample)
        self.session.commit()
    
    def add_run_id(self) -> None:
        for successor in self.successors:
            if isinstance(successor, BaseResourceNode):
                node_id = self.session.query(Node).filter_by(name=successor.name).first().id
                samples = self.session.query(Sample).filter_by(node_id=node_id, run_id=None).all()
                for sample in samples:
                    sample.run_id = self.get_run_id()
                    self.session.commit()

    def add_end_time(self) -> None:
        run_id = self.get_run_id()
        for successor in self.successors:
            if isinstance(successor, BaseResourceNode):
                node_id = self.session.query(Node).filter_by(name=successor.name).first().id
                samples = self.session.query(Sample).filter_by(node_id=node_id, run_id=run_id, end_time=None).all()
                for sample in samples:
                    sample.end_time = datetime.utcnow()
                    self.session.commit()

    def start_run(self) -> None:
        run = Run()
        self.session.add(run)
        self.session.commit()
        self.log(f"--------------------------- started run {run.id} at {datetime.now()}")
    
    def end_run(self) -> None:
        run = self.session.query(Run).filter_by(end_time=None).first()
        run.end_time = datetime.utcnow()
        self.session.commit()
        self.log(f"--------------------------- ended run {run.id} at {datetime.now()}")

    def on_exit(self):
        self.session.close()

