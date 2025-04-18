from typing import List, Dict, Union
from logging import Logger

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from anacostia_pipeline.nodes.metadata.sql.node import BaseSQLMetadataStoreNode
from anacostia_pipeline.nodes.metadata.sql.models import Base   # This is our declarative base



class SQLiteMetadataStoreNode(BaseSQLMetadataStoreNode):
    def __init__(
        self,
        name: str,
        uri: str,
        remote_successors: List[str] = None,
        caller_url: str = None,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
        super().__init__(name, uri, remote_successors=remote_successors, caller_url=caller_url, loggers=loggers)
        self.engine = create_engine(uri, connect_args={"check_same_thread": False}, echo=False, future=True)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        Base.metadata.create_all(bind=self._engine)

    def get_session(self):
        """
        Returns a new SQLAlchemy session.
        """
        return self.Session()