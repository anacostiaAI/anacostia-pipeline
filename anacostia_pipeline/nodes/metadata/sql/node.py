from typing import List, Dict, Union
from logging import Logger
from abc import ABC, abstractmethod
from contextlib import contextmanager
import traceback

from sqlalchemy.orm import sessionmaker, scoped_session, Session

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode



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
    