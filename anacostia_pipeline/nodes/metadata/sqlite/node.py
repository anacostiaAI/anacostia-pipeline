from datetime import datetime
from logging import Logger
import os
import sqlite3
from typing import List, Dict

from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.sqlite.gui import SqliteMetadataStoreGUI
from anacostia_pipeline.nodes.metadata.sqlite.rpc import SqliteMetadataRPCCallee



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

            cursor.execute("""
                CREATE TABLE triggers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_triggered INTEGER DEFAULT NULL,
                    node_id INTEGER,
                    trigger_time DATETIME,
                    message TEXT DEFAULT NULL,
                    FOREIGN KEY(node_id) REFERENCES nodes(id)
                )
            """)
    
    def add_node(self, node_name: str, node_type: str) -> None:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute(
                """INSERT INTO nodes(node_name, node_type, init_time) VALUES (?, ?, ?)""", 
                (node_name, node_type, datetime.now(),)
            )
    
    def start_run(self) -> None:
        with DatabaseManager(self.uri) as cursor:
            run_id = self.get_run_id()
            start_time = datetime.now()
            cursor.execute("INSERT INTO runs(run_id, start_time) VALUES (?, ?)", (run_id, start_time,))
            cursor.execute("UPDATE artifacts SET run_id = ?, state = 'current' WHERE run_id IS NULL AND state = 'new' ", (run_id,))
            cursor.execute("UPDATE triggers SET run_triggered = ? WHERE run_triggered IS NULL AND trigger_time < ?", (run_id, start_time,)) 

        self.log(f"--------------------------- started run {run_id} at {start_time}")
    
    def end_run(self) -> None:
        with DatabaseManager(self.uri) as cursor:
            end_time = datetime.now()
            cursor.execute("UPDATE runs SET end_time = ? WHERE end_time IS NULL", (end_time,))
            cursor.execute("UPDATE artifacts SET end_time = ?, state = 'old' WHERE end_time IS NULL AND state = 'current' ", (end_time,))

        self.log(f"--------------------------- ended run {self.get_run_id()} at {end_time}")

    def get_node_id(self, node_name: str) -> int:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute("SELECT id FROM nodes WHERE node_name = ?", (node_name,))
            result = cursor.fetchone()
            
            if result is None:
                raise ValueError(f"Node name '{node_name}' does not exist in the nodes table.")
            
            return result[0]
    
    def get_nodes_info(self, node_id: int = None, node_name: str = None) -> List[Dict]:
        if node_id is not None:
            sample_query = "SELECT * FROM nodes WHERE id = ?"
            sample_args = (node_id,)
        elif node_name is not None:
            sample_query = "SELECT * FROM nodes WHERE node_name = ?"
            sample_args = (node_name,)
        else:
            sample_query = "SELECT * FROM nodes"
            sample_args = ()
        
        with DatabaseManager(self.uri) as cursor:
            cursor.execute(sample_query, sample_args)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def create_entry(self, resource_node_name: str, filepath: str, state: str = "new", run_id: int = None) -> None:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute(
                """
                INSERT INTO artifacts (run_id, node_id, location, created_at, state)
                VALUES (?, (SELECT id FROM nodes WHERE node_name = ?), ?, ?, ?);
                """, (run_id, resource_node_name, filepath, datetime.now(), state)
            )

    def entry_exists(self, resource_node_name: BaseResourceNode, filepath: str) -> bool:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute("""
                SELECT 1
                FROM artifacts
                JOIN nodes ON artifacts.node_id = nodes.id
                WHERE artifacts.location = ? AND nodes.node_name = ?
                LIMIT 1;
            """, (filepath, resource_node_name))
            return cursor.fetchone() is not None

    def get_num_entries(self, resource_node_name: str, state: str = "all") -> int:

        with DatabaseManager(self.uri) as cursor:
            if state == "all":
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM artifacts
                    JOIN nodes ON artifacts.node_id = nodes.id
                    WHERE nodes.node_name = ?;
                """, (resource_node_name,))
            else:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM artifacts
                    JOIN nodes ON artifacts.node_id = nodes.id
                    WHERE nodes.node_name = ? AND state = ?;
                """, (resource_node_name, state,))

            return cursor.fetchone()[0]
    
    def get_entries(self, resource_node_name: str = None, state: str = "all") -> List[Dict]:
        main_query = """
            SELECT
                artifacts.id,
                artifacts.run_id,
                artifacts.location,
                artifacts.created_at,
                artifacts.end_time,
                artifacts.state,
                nodes.node_name
            FROM artifacts
            JOIN nodes ON artifacts.node_id = nodes.id
        """

        with DatabaseManager(self.uri) as cursor:
            if resource_node_name is not None and state != "all":
                cursor.execute(f"""
                    { main_query } 
                    WHERE nodes.node_name = ? AND artifacts.state = ?
                """, (resource_node_name, state))
            
            elif resource_node_name is not None:
                cursor.execute(f"""
                    { main_query } 
                    WHERE nodes.node_name = ?
                """, (resource_node_name,))
            
            elif state != "all":
                cursor.execute(f"""
                    { main_query } 
                    WHERE artifacts.state = ?
                """, (state,))
            
            else:
                cursor.execute(main_query)

            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    def get_runs(self) -> List[Dict]:
        with DatabaseManager(self.uri) as cursor:
            cursor.execute("SELECT * FROM runs")
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    def log_metrics(self, node_name: str, **kwargs) -> None:
        run_id = self.get_run_id()
        node_id = self.get_node_id(node_name)

        if not kwargs:
            return  # Avoid empty inserts

        with DatabaseManager(self.uri) as cursor:
            cursor.executemany(
                "INSERT INTO metrics (run_id, node_id, metric_name, metric_value) VALUES (?, ?, ?, ?)",
                [(run_id, node_id, metric_name, metric_value) for metric_name, metric_value in kwargs.items()]
            )

    def log_params(self, node_name: str, **kwargs) -> None:
        run_id = self.get_run_id()
        node_id = self.get_node_id(node_name)

        if not kwargs:
            return  # Avoid empty inserts

        with DatabaseManager(self.uri) as cursor:
            cursor.executemany(
                "INSERT INTO params(run_id, node_id, param_name, param_value) VALUES (?, ?, ?, ?)", 
                [(run_id, node_id, param_name, param_value) for param_name, param_value in kwargs.items()]
            )
    
    def set_tags(self, node_name: str, **kwargs) -> None:
        run_id = self.get_run_id()
        node_id = self.get_node_id(node_name)

        if not kwargs:
            return  # Avoid empty inserts

        with DatabaseManager(self.uri) as cursor:
            cursor.executemany(
                "INSERT INTO tags(run_id, node_id, tag_name, tag_value) VALUES (?, ?, ?, ?)", 
                [(run_id, node_id, tag_name, tag_value) for tag_name, tag_value in kwargs.items()]
            )
    
    def get_metrics(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        main_query = """
            SELECT
                metrics.id,
                metrics.run_id,
                metrics.metric_name,
                metrics.metric_value,
                nodes.node_name
            FROM metrics
            JOIN nodes ON metrics.node_id = nodes.id
        """

        with DatabaseManager(self.uri) as cursor:
            if run_id is not None and node_name is not None:
                cursor.execute(f"""
                    { main_query } 
                    WHERE metrics.run_id = ? AND node_id = ?
                """, (run_id, node_name))
            
            elif run_id is not None:
                cursor.execute(f"""
                    { main_query } 
                    WHERE metrics.run_id = ?
                """, (run_id,))
            
            elif node_name is not None:
                cursor.execute(f"""
                    { main_query } 
                    WHERE nodes.node_name = ?
                """, (node_name,))
            
            else:
                cursor.execute(main_query)

            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_params(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        main_query = """
            SELECT
                params.id,
                params.run_id,
                params.param_name,
                params.param_value,
                nodes.node_name
            FROM params
            JOIN nodes ON params.node_id = nodes.id
        """

        with DatabaseManager(self.uri) as cursor:
            if run_id is not None and node_name is not None:
                cursor.execute(f"""
                    { main_query }
                    WHERE params.run_id = ? AND nodes.node_name = ?
                """, (run_id, node_name,))

            elif run_id is not None:
                cursor.execute(f"""
                    { main_query }
                    WHERE params.run_id = ?
                """, (run_id,))

            elif node_name is not None:
                cursor.execute(f"""
                    { main_query }
                    WHERE nodes.node_name = ?
                """, (node_name,))

            else:
                cursor.execute(main_query)

            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_tags(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        main_query = """
            SELECT
                tags.id,
                tags.run_id,
                tags.tag_name,
                tags.tag_value,
                nodes.node_name
            FROM tags
            JOIN nodes ON tags.node_id = nodes.id
        """

        with DatabaseManager(self.uri) as cursor:
            if run_id is not None and node_name is not None:
                cursor.execute(f"""
                    { main_query }
                    WHERE tags.run_id = ? AND nodes.node_name = ?
                """, (run_id, node_name,))

            elif run_id is not None:
                cursor.execute(f"""
                    { main_query }
                    WHERE tags.run_id = ?
                """, (run_id,))

            elif node_name is not None:
                cursor.execute(f"""
                    { main_query }
                    WHERE nodes.node_name = ?
                """, (node_name,))
            else:
                cursor.execute(main_query)

            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def log_trigger(self, node_name: str, message: str = None) -> None:
        if message is not None:
            with DatabaseManager(self.uri) as cursor:
                cursor.execute(
                    """
                    INSERT INTO triggers(node_id, trigger_time, message) 
                    VALUES ((SELECT id FROM nodes WHERE node_name = ?), ?, ?)
                    """, (node_name, datetime.now(), message)
                )
    
    def get_triggers(self, node_name: str = None) -> List[Dict]:
        main_query = """
            SELECT 
                triggers.id,
                triggers.run_triggered,
                triggers.trigger_time,
                triggers.message,
                nodes.node_name
            FROM triggers
            JOIN nodes ON triggers.node_id = nodes.id;
        """

        with DatabaseManager(self.uri) as cursor:
            if node_name is not None:
                cursor.execute(f"""
                    { main_query }
                    WHERE nodes.node_name = ?;
                """, (node_name,))
            else:
                cursor.execute(main_query)

            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]



if __name__ == "__main__":
    node = SqliteMetadataStoreNode(name="sqlite_metadata_store", uri="metadata.db")
    node.setup()
    node.add_node(BaseResourceNode(name="resource_node", resource_path="file.txt", metadata_store=node))