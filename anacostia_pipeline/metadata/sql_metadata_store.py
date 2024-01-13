from logging import Logger
from typing import List, Dict
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from datetime import datetime
import os
from contextlib import contextmanager
import traceback

from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request, APIRouter

from ..engine.base import BaseMetadataStoreNode, BaseResourceNode, BaseNode, BaseActionNode



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

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Node(Base):
    __tablename__ = 'nodes'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)
    init_time = Column(DateTime, default=datetime.utcnow)


@contextmanager
def scoped_session_manager(session_factory: sessionmaker, node: BaseNode) -> scoped_session:
    ScopedSession = scoped_session(session_factory)
    session = ScopedSession()

    try:
        yield session
    except Exception as e:
        node.log(traceback.format_exc(), level="ERROR")
        session.rollback()
        node.log(f"Node {node.name} rolled back session.", level="ERROR")
        raise e
    finally:
        ScopedSession.close()


class SqliteMetadataStore(BaseMetadataStoreNode):
    def __init__(self, name: str, uri: str, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, uri, loggers)

        # to override the default router, create a new router and assign routes to it
        # Note: declare the router after the super().__init__() call
        # Note: routes must be assigned in the __init__() method
        # Note: if we don't declare a new router, the default router 
        # (and all the routes associated with the default router) in BaseNode will be used
        self.router = APIRouter()

        @self.router.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            return f"<p>hello from node {name}</p>"

    def setup(self) -> None:
        path = self.uri.strip('sqlite:///')
        path = path.split('/')[0:-1]
        path = '/'.join(path)
        if os.path.exists(path) is False:
            os.makedirs(path, exist_ok=True)

        # Create an engine that stores data in the local directory's sqlite.db file.
        engine = create_engine(f'{self.uri}', connect_args={"check_same_thread": False})

        # Create all tables in the engine (this is equivalent to "Create Table" statements in raw SQL).
        Base.metadata.create_all(engine)

        # Create a sessionmaker, binding it to the engine
        self.session_factory = sessionmaker(bind=engine)

    def get_run_id(self) -> int:
        with scoped_session_manager(self.session_factory, self) as session:
            run = session.query(Run).filter_by(end_time=None).first()
            return run.id
    
    def get_num_entries(self, resource_node: BaseResourceNode, state: str) -> int:
        # add some assertion statements here to check if state is "new", "current", "old", or "all"
        with scoped_session_manager(self.session_factory, resource_node) as session:
            node_id = session.query(Node).filter_by(name=resource_node.name).first().id
            if state == "all":
                return session.query(Sample).filter_by(node_id=node_id).count()
            else:
                return session.query(Sample).filter_by(node_id=node_id, state=state).count()
    
    def create_resource_tracker(self, resource_node: BaseResourceNode) -> None:
        with scoped_session_manager(self.session_factory, resource_node) as session:
            resource_name = resource_node.name
            type_name = type(resource_node).__name__
            node = Node(name=resource_name, type=type_name)
            session.add(node)
            session.commit()

    def log_metrics(self, **kwargs) -> None:
        with scoped_session_manager(self.session_factory, self) as session:
            run_id = self.get_run_id()
            for key, value in kwargs.items():
                metric = Metric(run_id=run_id, key=key, value=value)
                session.add(metric)
            session.commit()
    
    def log_params(self, **kwargs) -> None:
        with scoped_session_manager(self.session_factory, self) as session:
            run_id = self.get_run_id()
            for key, value in kwargs.items():
                param = Param(run_id=run_id, key=key, value=value)
                session.add(param)
            session.commit()
    
    def set_tags(self, **kwargs) -> None:
        with scoped_session_manager(self.session_factory, self) as session:
            run_id = self.get_run_id()
            for key, value in kwargs.items():
                tag = Tag(run_id=run_id, key=key, value=value)
                session.add(tag)
            session.commit()

    def get_entries(self, resource_node: BaseResourceNode, state: str) -> Dict[str, Sample]:
        with scoped_session_manager(self.session_factory, resource_node) as session:
            node_id = session.query(Node).filter_by(name=resource_node.name).first().id
            return session.query(Sample).filter_by(node_id=node_id, state=state).all()

    def entry_exists(self, resource_node: BaseResourceNode, filepath: str) -> bool:
        with scoped_session_manager(self.session_factory, resource_node) as session:
            node_id = session.query(Node).filter_by(name=resource_node.name).first().id
            return session.query(Sample).filter_by(node_id=node_id, location=filepath).count() > 0

    def create_entry(self, resource_node: BaseResourceNode, filepath: str, state: str = "new", run_id: int = None) -> None:
        with scoped_session_manager(self.session_factory, resource_node) as session:
            # in the future, refactor this by changing filepath to uri 
            node_id = session.query(Node).filter_by(name=resource_node.name).first().id
            sample = Sample(node_id=node_id, location=filepath, state=state, run_id=run_id)
            session.add(sample)
            session.commit()
    
    def add_run_id(self) -> None:
        with scoped_session_manager(self.session_factory, self) as session:
            for successor in self.successors:
                if isinstance(successor, BaseResourceNode):
                    node_id = session.query(Node).filter_by(name=successor.name).first().id
                    samples = session.query(Sample).filter_by(node_id=node_id, run_id=None).all()
                    for sample in samples:
                        sample.run_id = self.get_run_id()
                        sample.state = "current"
                        session.commit()

    def add_end_time(self) -> None:
        with scoped_session_manager(self.session_factory, self) as session:
            run_id = self.get_run_id()
            for successor in self.successors:
                if isinstance(successor, BaseResourceNode):
                    node_id = session.query(Node).filter_by(name=successor.name).first().id
                    samples = session.query(Sample).filter_by(node_id=node_id, run_id=run_id, end_time=None).all()
                    for sample in samples:
                        sample.end_time = datetime.utcnow()
                        sample.state = "old"
                        session.commit()

    def start_run(self) -> None:
        with scoped_session_manager(self.session_factory, self) as session:
            run = Run()
            session.add(run)
            session.commit()
            self.log(f"--------------------------- started run {run.id} at {datetime.now()}")
    
    def end_run(self) -> None:
        with scoped_session_manager(self.session_factory, self) as session:
            run: Run = session.query(Run).filter_by(end_time=None).first()
            run.end_time = datetime.utcnow()
            session.commit()
            self.log(f"--------------------------- ended run {run.id} at {datetime.now()}")
