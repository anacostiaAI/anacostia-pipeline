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
    created_at = Column(DateTime, default=datetime.utcnow)

class Node(Base):
    __tablename__ = 'nodes'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)
    state = Column(String, default="new")
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
    
    def create_resource_tracker(self, resource_node: BaseResourceNode) -> None:
        resource_name = resource_node.name
        type_name = type(resource_node).__name__
        node = Node(name=resource_name, type=type_name)
        self.session.add(node)
        self.session.commit()

    def on_exit(self):
        self.session.close()

