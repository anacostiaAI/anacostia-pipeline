from datetime import datetime
from logging import Logger
import os
import sqlite3
from typing import List, Dict
from threading import Thread
import traceback

from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.sqlite.gui import SqliteMetadataStoreGUI
from anacostia_pipeline.nodes.metadata.sqlite.rpc import SqliteMetadataRPCCallee
from anacostia_pipeline.nodes.node import BaseNode



def convert_datetime(sqlite_datetime_bytes: bytes) -> datetime:
    """
    Converter function for SQLite datetime bytes to Python datetime object.
    Preserves microseconds precision from the SQLite datetime string.
    
    Args:
        sqlite_datetime_bytes: Bytes object containing datetime string in format 'YYYY-MM-DD HH:MM:SS.mmmmmm'
        
    Returns:
        datetime.datetime object or None if input is None/empty
    """

    if sqlite_datetime_bytes is None:
        return None
    
    sqlite_datetime = sqlite_datetime_bytes.decode('utf-8')     # Decode bytes to string
    
    return datetime.strptime(sqlite_datetime, '%Y-%m-%d %H:%M:%S.%f')


# Register custom converter for DATETIME type.
sqlite3.register_converter('DATETIME', convert_datetime)



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
    def __init__(self, name: str, uri: str, remote_successors: List[str] = None, caller_url: str = None, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, uri, remote_successors=remote_successors, caller_url=caller_url, loggers=loggers)
    
    # Note: override the get_app() method to return the custom router
    def setup_node_GUI(self) -> SqliteMetadataStoreGUI:
        return SqliteMetadataStoreGUI(self)
    
    def setup_rpc_callee(self, host, port):
        self.rpc_callee = SqliteMetadataRPCCallee(self, self.caller_url, host, port, loggers=self.loggers)
        return self.rpc_callee

    def setup(self) -> None:
        directory = os.path.dirname(self.uri)
        if directory != "" and os.path.exists(directory) is False:
            os.makedirs(directory)

        with DatabaseManager(self.uri) as cursor:

            # create the runs table
            cursor.execute("""
                CREATE TABLE runs (
                    run_id INTEGER PRIMARY KEY, 
                    start_time DATETIME, 
                    end_time DATETIME DEFAULT NULL
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

            # create the metrics table
            cursor.execute("""
                CREATE TABLE metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    run_id INTEGER,
                    node_id INTEGER,
                    metric_name TEXT, 
                    metric_value REAL, 
                    FOREIGN KEY(run_id) REFERENCES runs(run_id),
                    FOREIGN KEY(node_id) REFERENCES nodes(id)
                )
            """)

            # create the tags table
            cursor.execute("""
                CREATE TABLE tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    run_id INTEGER,
                    node_id INTEGER,
                    tag_name TEXT, 
                    tag_value TEXT, 
                    FOREIGN KEY(run_id) REFERENCES runs(run_id),
                    FOREIGN KEY(node_id) REFERENCES nodes(id)
                )
            """)

            # create the params table
            cursor.execute("""
                CREATE TABLE params (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    run_id INTEGER,
                    node_id INTEGER,
                    param_name TEXT, 
                    param_value TEXT, 
                    FOREIGN KEY(run_id) REFERENCES runs(run_id),
                    FOREIGN KEY(node_id) REFERENCES nodes(id)
                )
            """)

            # create the artifacts table
            cursor.execute("""
                CREATE TABLE artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    run_id INTEGER DEFAULT NULL,
                    node_id INTEGER,
                    location TEXT, 
                    created_at DATETIME,
                    end_time DATETIME DEFAULT NULL,
                    state TEXT DEFAULT 'new',
                    FOREIGN KEY(run_id) REFERENCES runs(run_id),
                    FOREIGN KEY(node_id) REFERENCES nodes(id)
                )
            """)
    
    def start_monitoring(self) -> None:

        def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'")
            while self.exit_event.is_set() is False:
                if self.exit_event.is_set() is True: break
                try:
                    self.custom_trigger()
                
                except NotImplementedError:
                    self.base_trigger()

                except Exception as e:
                        self.log(f"Error checking resource in node '{self.name}': {traceback.format_exc()}")
                
        self.observer_thread = Thread(name=f"{self.name}_observer", target=_monitor_thread_func)
        self.observer_thread.start()

    def stop_monitoring(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer_thread.join()
        self.log(f"Observer stopped for node '{self.name}'")

    def custom_trigger(self) -> bool:
        """
        Override to implement your custom triggering logic. If the custom_trigger method is not implemented, the base_trigger method will be called.
        """
        raise NotImplementedError
    
    def base_trigger(self) -> None:
        """
        The default trigger for the SqliteMetadataStoreNode. 
        base_trigger does not check any metric, it just simply triggers the pipeline.
        base_trigger is called when the custom_trigger method is not implemented.
        """
        self.trigger()
    
    def add_node(self, node: BaseNode) -> None:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute(
                """INSERT INTO nodes(node_name, node_type, init_time) VALUES (?, ?, ?)""", 
                (node.name, type(node).__name__, datetime.now(),)
            )
    
    def start_run(self) -> None:
        with DatabaseManager(self.uri) as cursor:
            run_id = self.get_run_id()
            cursor.execute("INSERT INTO runs(run_id, start_time) VALUES (?, ?)", (run_id, datetime.now(),))
            cursor.execute("UPDATE artifacts SET run_id = ?, state = 'current' WHERE run_id IS NULL AND state = 'new' ", (run_id,))

        self.log(f"--------------------------- started run {run_id} at {datetime.now()}")
    
    def end_run(self) -> None:
        with DatabaseManager(self.uri) as cursor:
            end_time = datetime.now()
            cursor.execute("UPDATE runs SET end_time = ? WHERE end_time IS NULL", (end_time,))
            cursor.execute("UPDATE artifacts SET end_time = ?, state = 'old' WHERE end_time IS NULL AND state = 'current' ", (end_time,))
    
        self.log(f"--------------------------- ended run {self.get_run_id()} at {datetime.now()}")

    def get_node_id(self, node: BaseNode) -> int:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute("SELECT id FROM nodes WHERE node_name = ?", (node.name,))
            return cursor.fetchone()[0]
    
    def get_nodes_info(self, node_id: int = None, node: BaseNode = None) -> List[Dict]:
        if node_id is not None:
            sample_query = "SELECT * FROM nodes WHERE id = ?"
            sample_args = (node_id,)
        elif node is not None:
            sample_query = "SELECT * FROM nodes WHERE node_name = ?"
            sample_args = (node.name,)
        else:
            sample_query = "SELECT * FROM nodes"
            sample_args = ()
        
        with DatabaseManager(self.uri) as cursor:
            cursor.execute(sample_query, sample_args)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def create_entry(self, resource_node: BaseResourceNode, filepath: str, state: str = "new", run_id: int = None) -> None:
        node_id = self.get_node_id(resource_node)

        with DatabaseManager(self.uri) as cursor:
            cursor.execute(
                "INSERT INTO artifacts(run_id, node_id, location, created_at, state) VALUES (?, ?, ?, ?, ?)", 
                (run_id, node_id, filepath, datetime.now(), state)
            )

    def entry_exists(self, resource_node: BaseResourceNode, filepath: str) -> bool:
        node_id = self.get_node_id(resource_node)

        with DatabaseManager(self.uri) as cursor:
            cursor.execute("SELECT * FROM artifacts WHERE node_id = ? AND location = ?", (node_id, filepath))
            return cursor.fetchone() is not None

    def get_num_entries(self, resource_node: BaseResourceNode, state: str = "all") -> int:
        node_id = self.get_node_id(resource_node)

        with DatabaseManager(self.uri) as cursor:
            if state == "all":
                cursor.execute("SELECT COUNT(id) FROM artifacts WHERE node_id = ?", (node_id,))
            else:
                cursor.execute("SELECT COUNT(id) FROM artifacts WHERE node_id = ? AND state = ?", (node_id, state))
            return cursor.fetchone()[0]
    
    def get_entries(self, resource_node: BaseResourceNode = "all", state: str = "all") -> List[Dict]:
        if (resource_node != "all") and (state != "all"):
            node_id = self.get_node_id(resource_node)
            sample_query = "SELECT * FROM artifacts WHERE node_id = ? AND state = ?"
            sample_args = (node_id, state,)
        elif (resource_node != "all") and (state == "all"):
            node_id = self.get_node_id(resource_node)
            sample_query = "SELECT * FROM artifacts WHERE node_id = ?"
            sample_args = (node_id,)
        elif (resource_node == "all") and (state != "all"):
            sample_query = "SELECT * FROM artifacts WHERE state = ?"
            sample_args = (state,)
        else:
            sample_query = "SELECT * FROM artifacts"
            sample_args = ()
        
        with DatabaseManager(self.uri) as cursor:
            cursor.execute(sample_query, sample_args)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_runs(self) -> List[Dict]:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute("SELECT * FROM runs")
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    def log_metrics(self, node: BaseNode, **kwargs) -> None:
        run_id = self.get_run_id()
        node_id = self.get_node_id(node)
        with DatabaseManager(self.uri) as cursor:
            for metric_name, metric_value in kwargs.items():
                cursor.execute(
                    "INSERT INTO metrics(run_id, node_id, metric_name, metric_value) VALUES (?, ?, ?, ?)", 
                    (run_id, node_id, metric_name, metric_value)
                )
    
    def log_params(self, node: BaseNode, **kwargs) -> None:
        run_id = self.get_run_id()
        node_id = self.get_node_id(node)
        with DatabaseManager(self.uri) as cursor:
            for param_name, param_value in kwargs.items():
                cursor.execute(
                    "INSERT INTO params(run_id, node_id, param_name, param_value) VALUES (?, ?, ?, ?)", 
                    (run_id, node_id, param_name, param_value)
                )
    
    def set_tags(self, node: BaseNode, **kwargs) -> None:
        run_id = self.get_run_id()
        node_id = self.get_node_id(node)
        with DatabaseManager(self.uri) as cursor:
            for tag_name, tag_value in kwargs.items():
                cursor.execute(
                    "INSERT INTO tags(run_id, node_id, tag_name, tag_value) VALUES (?, ?, ?, ?)", 
                    (run_id, node_id, tag_name, tag_value)
                )
    
    def get_metrics(self, run_id: int = None, node: BaseNode = None) -> List[Dict]:
        node_id = self.get_node_id(node) if node is not None else None

        with DatabaseManager(self.uri) as cursor:
            if run_id is not None and node_id is not None:
                cursor.execute("SELECT * FROM metrics WHERE run_id = ? AND node_id = ?", (run_id, node_id))
            elif run_id is not None:
                cursor.execute("SELECT * FROM metrics WHERE run_id = ?", (run_id,))
            elif node_id is not None:
                cursor.execute("SELECT * FROM metrics WHERE node_id = ?", (node_id,))
            else:
                cursor.execute("SELECT * FROM metrics")

            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_params(self, run_id: int = None, node: BaseNode = None) -> List[Dict]:
        node_id = self.get_node_id(node) if node is not None else None

        with DatabaseManager(self.uri) as cursor:
            if run_id is not None and node_id is not None:
                cursor.execute("SELECT * FROM params WHERE run_id = ? AND node_id = ?", (run_id, node_id))
            elif run_id is not None:
                cursor.execute("SELECT * FROM params WHERE run_id = ?", (run_id,))
            elif node_id is not None:
                cursor.execute("SELECT * FROM params WHERE node_id = ?", (node_id,))
            else:
                cursor.execute("SELECT * FROM params")

            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_tags(self, run_id: int = None, node: BaseNode = None) -> List[Dict]:
        node_id = self.get_node_id(node) if node is not None else None

        with DatabaseManager(self.uri) as cursor:
            if run_id is not None and node_id is not None:
                cursor.execute("SELECT * FROM tags WHERE run_id = ? AND node_id = ?", (run_id, node_id))
            elif run_id is not None:
                cursor.execute("SELECT * FROM tags WHERE run_id = ?", (run_id,))
            elif node_id is not None:
                cursor.execute("SELECT * FROM tags WHERE node_id = ?", (node_id,))
            else:
                cursor.execute("SELECT * FROM tags")

            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]



if __name__ == "__main__":
    node = SqliteMetadataStoreNode(name="sqlite_metadata_store", uri="metadata.db")
    node.setup()
    node.add_node(BaseResourceNode(name="resource_node", resource_path="file.txt", metadata_store=node))