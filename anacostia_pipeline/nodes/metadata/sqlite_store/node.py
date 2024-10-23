from datetime import datetime, timezone
from logging import Logger
import os
import sqlite3
from typing import List

from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode



class DatabaseManager:
    def __init__(self, db_path: str):
        # Initialize with database path.
        self.db_path = db_path
        self._connection = None
        self._cursor = None
    
    def __enter__(self):
        # Create and return database cursor when entering context.

        self._connection = sqlite3.connect(
            database=self.db_path,
            check_same_thread=False,
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        self._cursor = self._connection.cursor()
        return self._cursor
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Handle cleanup when exiting context.

        if self._cursor is not None:
            self._cursor.close()
            
        if self._connection is not None:
            try:
                if exc_type is None:
                    self._connection.commit()               # No error occurred - commit changes
                else:
                    self._connection.rollback()             # Error occurred - rollback changes 
                    print(f"Exception type: {exc_type}")    # The class of the exception
                    print(f"Exception value: {exc_val}")    # The actual error message/details
                    print(f"Traceback: {exc_tb}")           # Where the error occurred
            finally:
                self._connection.close()
                
        return False  # Don't suppress exceptions


class SqliteMetadataStoreNode(BaseMetadataStoreNode):
    def __init__(self, name: str, uri: str, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, uri, loggers)
    
    def setup(self) -> None:
        directory = os.path.dirname(self.uri)
        if directory != "" and os.path.exists(directory) is False:
            os.makedirs(directory)

        with DatabaseManager(self.uri) as cursor:

            # create the runs table
            cursor.execute("""
                CREATE TABLE runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    start_time DATETIME, 
                    end_time DATETIME DEFAULT NULL
                )
            """)

            # create the metrics table
            cursor.execute("""
                CREATE TABLE metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    run_id INTEGER,
                    metric_name TEXT, 
                    metric_value REAL, 
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                )
            """)

            # create the tags table
            cursor.execute("""
                CREATE TABLE tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    run_id INTEGER,
                    tag_name TEXT, 
                    tag_value TEXT, 
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                )
            """)

            # create the params table
            cursor.execute("""
                CREATE TABLE params (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    run_id INTEGER,
                    param_name TEXT, 
                    param_value TEXT, 
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                )
            """)

            # create the nodes table
            cursor.execute("""
                CREATE TABLE nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    node_name TEXT, 
                    node_type TEXT,
                    init_time DATETIME
                )
            """)

            # create the artifacts table
            cursor.execute("""
                CREATE TABLE artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    run_id INTEGER DEFAULT NULL,
                    node_id INTEGER,
                    artifact_path TEXT, 
                    created_at DATETIME,
                    end_time DATETIME DEFAULT NULL,
                    state TEXT DEFAULT 'new',
                    FOREIGN KEY(run_id) REFERENCES runs(run_id),
                    FOREIGN KEY(node_id) REFERENCES nodes(id)
                )
            """)
    
    def create_resource_tracker(self, resource_node: BaseResourceNode) -> None:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute(
                """INSERT INTO nodes(node_name, node_type, init_time) VALUES (?, ?, ?)""", 
                (resource_node.name, type(resource_node).__name__, datetime.now(timezone.utc),)
            )

    def get_run_id(self) -> int:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute("SELECT run_id FROM runs WHERE end_time IS NULL")
            return cursor.fetchone()[0]
    
    def start_run(self) -> None:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute("INSERT INTO runs(start_time) VALUES (?)", (datetime.now(timezone.utc),))
        
        run_id = self.get_run_id()
        self.log(f"--------------------------- started run {run_id} at {datetime.now(timezone.utc)}")
    
    def end_run(self) -> None:
        run_id = self.get_run_id()
        self.log(f"--------------------------- ended run {run_id} at {datetime.now(timezone.utc)}")

        with DatabaseManager(self.uri) as cursor:
            cursor.execute("UPDATE runs SET end_time = ? WHERE end_time IS NULL", (datetime.now(timezone.utc),))
    
    def get_node_id(self, resource_node: BaseResourceNode) -> int:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute("SELECT id FROM nodes WHERE node_name = ?", (resource_node.name,))
            return cursor.fetchone()[0]
    
    def create_entry(self, resource_node: BaseResourceNode, filepath: str, state: str = "new", run_id: int = None) -> None:
        node_id = self.get_node_id(resource_node)

        with DatabaseManager(self.uri) as cursor:
            cursor.execute(
                "INSERT INTO artifacts(run_id, node_id, artifact_path, created_at, state) VALUES (?, ?, ?, ?, ?)", 
                (run_id, node_id, filepath, datetime.now(timezone.utc), state)
            )

    def entry_exists(self, resource_node: BaseResourceNode, filepath: str) -> bool:
        node_id = self.get_node_id(resource_node)

        with DatabaseManager(self.uri) as cursor:
            cursor.execute("SELECT * FROM artifacts WHERE node_id = ? AND artifact_path = ?", (node_id, filepath))
            return cursor.fetchone() is not None

    def get_num_entries(self, resource_node: BaseResourceNode, state: str = "all") -> int:
        node_id = self.get_node_id(resource_node)

        with DatabaseManager(self.uri) as cursor:
            if state == "all":
                cursor.execute("SELECT COUNT(id) FROM artifacts WHERE node_id = ?", (node_id,))
            else:
                cursor.execute("SELECT COUNT(id) FROM artifacts WHERE node_id = ? AND state = ?", (node_id, state))
            return cursor.fetchone()[0]
    
    def add_run_id(self) -> None:
        run_id = self.get_run_id()
        with DatabaseManager(self.uri) as cursor:
            cursor.execute(
                """UPDATE artifacts SET run_id = ?, state = 'current' WHERE run_id IS NULL AND state = 'new' """, 
                (run_id,)
            )

    def add_end_time(self) -> None:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute(
                """UPDATE artifacts SET end_time = ?, state = 'old' WHERE end_time IS NULL AND state = 'current' """, 
                (datetime.now(timezone.utc),)
            )


if __name__ == "__main__":
    node = SqliteMetadataStoreNode(name="sqlite_metadata_store", uri="metadata.db")
    node.setup()
    node.create_resource_tracker(BaseResourceNode(name="resource_node", resource_path="file.txt", metadata_store=node))