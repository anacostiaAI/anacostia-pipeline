from typing import List, Union
from logging import Logger
import os

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from anacostia_pipeline.nodes.metadata.sql.node import BaseSQLMetadataStoreNode
from anacostia_pipeline.nodes.metadata.sql.models import Base   # This is our declarative base


class SQLiteMetadataStoreNode(BaseSQLMetadataStoreNode):
    def __init__(
        self,
        name: str,
        uri: str,
        remote_successors: List[str] = None,
        client_url: str = None,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
        if uri.startswith("sqlite:///") is False:
            raise ValueError(f"Invalid URI: {uri}. SQLite URIs must start with 'sqlite:///'")

        super().__init__(name, uri, remote_successors=remote_successors, client_url=client_url, loggers=loggers)

    def setup(self):
        # create the folder where the SQLite database will be stored if it does not exist
        path = self.uri.strip('sqlite:///')
        path = path.split('/')[0:-1]
        path = '/'.join(path)
        if os.path.exists(path) is False:
            os.makedirs(path, exist_ok=True)
        
        # Create an engine that stores data in the local directory's sqlite.db file.
        engine = create_engine(
            self.uri, 
            connect_args={"check_same_thread": False}, 
            echo=False, 
            future=True
        )

        # Enable Write-Ahead Logging (WAL) mode
        # This is important for concurrent access to the SQLite database.
        # WAL mode allows multiple readers and a single writer, 
        # which is suitable for Anacostia where we have multiple threads that need to read from the database.
        with engine.connect() as conn:
            conn.execute(text("PRAGMA journal_mode=WAL"))
            conn.commit()

        # Create all tables in the engine (this is equivalent to "Create Table" statements in raw SQL).
        Base.metadata.create_all(bind=engine)

        # Create a sessionmaker, binding it to the engine
        self.session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        self.init_scoped_session(self.session_factory)