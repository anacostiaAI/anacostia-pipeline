from typing import List, Dict, Union
from logging import Logger
from abc import ABC, abstractmethod
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.orm import relationship

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode


Base = declarative_base()


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

    @abstractmethod
    def get_session(self) -> Session:
        """
        This method should be implemented by subclasses to provide a SQLAlchemy session objec for database operations.
        """
        pass
    
    @abstractmethod
    def setup_node_GUI(self):
        """
        Override to setup the node GUI.
        """
        pass

    @abstractmethod
    def setup_rpc_callee(self, host, port):
        """
        Override to setup the RPC callee.
        """
        pass
    
    def setup(self) -> None:
        directory = os.path.dirname(self.uri)
        if directory != "" and os.path.exists(directory) is False:
            os.makedirs(directory)
