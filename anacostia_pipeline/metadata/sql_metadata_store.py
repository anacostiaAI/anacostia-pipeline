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
    value = Column(String)

class Tag(Base):
    __tablename__ = 'tags'
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer)
    key = Column(String)
    value = Column(String)


class SqliteMetadataStore(BaseMetadataStoreNode):
    def __init__(self, name: str, uri: str, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, uri, loggers)
        self.session = None
    
    def setup(self) -> None:
        # Create an engine that stores data in the local directory's sqlite.db file.
        engine = create_engine(f'{self.uri}', echo=True)

        # Create all tables in the engine (this is equivalent to "Create Table" statements in raw SQL).
        Base.metadata.create_all(engine)

        # Create a sessionmaker, binding it to the engine
        Session = sessionmaker(bind=engine)
        self.session = Session()
    
    def on_exit(self):
        self.session.close()

