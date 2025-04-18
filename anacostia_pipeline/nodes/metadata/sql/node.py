from typing import List, Dict, Union
from logging import Logger
from abc import ABC, abstractmethod
from contextlib import contextmanager
import traceback
from datetime import datetime

from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy import exists, select

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.sql.models import Artifact, Metric, Param, Run, Tag, Trigger, Node



class BaseSQLMetadataStoreNode(BaseMetadataStoreNode, ABC):
    """
    Base class for SQL metadata store nodes.
    SQL metadata store nodes are nodes that are used to store metadata about the pipeline in a SQL database.
    The SQL metadata store node is a special type of resource node that will be the predecessor of all other resource nodes;
    thus, by extension, the SQL metadata store node will always be the root node of the DAG.
    The abstract methods in this class must be implemented by the subclasses to provide more specific functionality (e.g., setting up ).
    """

    def __init__(
        self,
        name: str,
        uri: str,
        remote_successors: List[str] = None,
        caller_url: str = None,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
        super().__init__(name, uri, remote_successors=remote_successors, caller_url=caller_url, loggers=loggers)
        self._ScopedSession: Session = None
    
    @abstractmethod
    def setup_node_GUI(self):
        """Override to setup the node GUI."""
        pass

    @abstractmethod
    def setup_rpc_callee(self, host: str, port: int):
        """Override to setup the RPC callee."""
        pass

    def init_scoped_session(self, session_factory: sessionmaker):
        """Call this from the child class after engine setup."""
        self._ScopedSession = scoped_session(session_factory)

    @contextmanager
    def get_session(self):
        session = self._ScopedSession()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.log(traceback.format_exc(), level="ERROR")
            self.log(f"Node {self.name} rolled back session.", level="ERROR")
            raise
        finally:
            self._ScopedSession.remove()

    def add_node(self, node_name: str, node_type: str) -> None:
        with self.get_session() as session:
            node = Node(node_name=node_name, node_type=node_type, init_time=datetime.now())
            session.add(node)
    
    def create_entry(
        self, resource_node_name: str, filepath: str, 
        state: str = "new", run_id: int = None, hash: str = None, file_size: int = None, type: str = None
    ) -> None:
        with self.get_session() as session:
            node = session.query(Node).filter_by(node_name=resource_node_name).first()
            if not node:
                raise ValueError(f"No node found with name: {resource_node_name}")

            entry = Artifact(
                run_id=run_id,
                node_id=node.id,
                location=filepath,
                created_at=datetime.now(),
                state=state,
                hash=hash,
                size=file_size,
                type=type
            )
            session.add(entry)
    
    def get_num_entries(self, resource_node_name: str, state: str) -> int:
        # Validate input
        valid_states = {"new", "current", "old", "all"}
        assert state in valid_states, f"Invalid state: '{state}'. Must be one of {valid_states}"

        with self.get_session() as session:
            node = session.query(Node).filter_by(node_name=resource_node_name).first()
            if not node:
                raise ValueError(f"No node found with name: {resource_node_name}")

            query = session.query(Artifact).filter_by(node_id=node.id)
            if state != "all":
                query = query.filter_by(state=state)

            return query.count()

    def entry_exists(self, resource_node_name: str, filepath: str) -> bool:
        with self.get_session() as session:
            node = session.query(Node).filter_by(node_name=resource_node_name).first()
            if not node:
                raise ValueError(f"No node found with name: {resource_node_name}")

            stmt = select(exists().where(
                Artifact.node_id == node.id,
                Artifact.location == filepath
            ))
            return session.execute(stmt).scalar()
    
    def start_run(self):
        pass
    
    def end_run(self):
        pass