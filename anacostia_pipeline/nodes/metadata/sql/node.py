from typing import List, Dict, Union
from logging import Logger
from abc import ABC, abstractmethod
from contextlib import contextmanager
import traceback
from datetime import datetime
import hashlib
import json

from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy import exists, select, update

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.sql.gui import SQLMetadataStoreGUI
from anacostia_pipeline.nodes.metadata.sql.api import SQLMetadataStoreServer
from anacostia_pipeline.nodes.metadata.sql.models import Artifact, Metric, Param, Run, Tag, Trigger, Node



class BaseSQLMetadataStoreNode(BaseMetadataStoreNode, ABC):
    """
    Base class for SQL metadata store nodes.
    SQL metadata store nodes are nodes that are used to store metadata about the pipeline in a SQL database.
    The SQL metadata store node is a special type of resource node that will be the predecessor of all other resource nodes;
    thus, by extension, the SQL metadata store node will always be the root node of the DAG.
    The abstract methods in this class must be implemented by child classes to provide more specific functionality 
    (e.g., setting check_same_thread=True when creating an engine for SQLite).
    """

    def __init__(
        self,
        name: str,
        uri: str,
        remote_successors: List[str] = None,
        client_url: str = None,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
        super().__init__(name, uri, remote_successors=remote_successors, client_url=client_url, loggers=loggers)
        self._ScopedSession: Session = None
    
    @abstractmethod
    def setup(self):
        """
        Setup the SQL metadata store node.
        This method should be overridden by child classes to provide specific functionality.
        For example, the SQLiteMetadataStoreNode class will create an engine with check_same_thread=True.
        """
        raise NotImplementedError("The setup method must be overridden in the child class.") 
    
    def setup_node_GUI(self, host: str, port: int):
        """Override to setup the node GUI."""
        self.gui = SQLMetadataStoreGUI(node=self, host=host, port=port)
        return self.gui

    def setup_node_server(self, host: str, port: int):
        """Override to setup the RPC server."""
        self.node_server = SQLMetadataStoreServer(self, self.client_url, host, port, loggers=self.loggers)
        return self.node_server

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
    
    def node_exists(self, node_name: str) -> bool:
        with self.get_session() as session:
            stmt = select(exists().where(Node.node_name == node_name))
            return session.execute(stmt).scalar()

    def add_node(self, node_name: str, node_type: str, base_type: str) -> None:
        if self.node_exists(node_name):
            raise ValueError(f"Node name '{node_name}' already exists in the nodes table.")

        with self.get_session() as session:
            node = Node(node_name=node_name, node_type=node_type, base_type=base_type, init_time=datetime.now())
            session.add(node)
    
    def start_run(self):
        run_id = self.get_run_id()
        start_time = datetime.now()

        with self.get_session() as session:
            # add a new run in the database
            run = Run(run_id=run_id, start_time=start_time)
            session.add(run)

            # update all artifacts with run_id = None and state = "new" to have the current run_id
            stmt_artifacts = (
                update(Artifact)
                .where(Artifact.run_id.is_(None), Artifact.state == "new")
                .values(run_id=run_id, state="current")
            )
            session.execute(stmt_artifacts)

            # Update triggers where run_triggered is NULL and trigger_time is earlier than this run
            # Note: there are instances where multiple triggers are required to trigger a run (e.g., a metric trigger and a resource trigger)
            stmt_triggers = (
                update(Trigger)
                .where(
                    Trigger.run_triggered.is_(None),
                    Trigger.trigger_time < start_time
                )
                .values(run_triggered=run_id)
            )
            session.execute(stmt_triggers)

        self.log(f"--------------------------- started run {run_id} at {start_time}")
    
    def hash_run_metadata(self, metrics: List[Dict], params: List[Dict], tags: List[Dict]) -> str:
        def stable_hash(records: List[Dict], sort_key: str) -> str:
            sorted_records = sorted(records, key=lambda r: r[sort_key])
            serialized = json.dumps(sorted_records, sort_keys=True)
            return hashlib.sha256(serialized.encode()).hexdigest()

        metrics_hash = stable_hash(metrics, sort_key="id")
        params_hash = stable_hash(params, sort_key="id")
        tags_hash = stable_hash(tags, sort_key="id")

        # Combine into a final hash for the run
        combined = metrics_hash + params_hash + tags_hash
        return hashlib.sha256(combined.encode()).hexdigest()

    def end_run(self) -> None:
        end_time = datetime.now()

        # Create the hash for the run by retrieving the hashes of all artifacts, metrics, params, and tags associated with the current run into a list,
        # sorting the list, concatenating the hashes in the list into a string, and then hashing the string
        entries = self.get_entries(state="current")
        artifact_hashes = [entry["hash"] for entry in entries]
        artifact_hashes = ''.join(sorted(artifact_hashes))

        run_metadata_hash = self.hash_run_metadata(
            metrics=self.get_metrics(run_id=self.get_run_id()),
            params=self.get_params(run_id=self.get_run_id()),
            tags=self.get_tags(run_id=self.get_run_id())
        )

        combined_hash = artifact_hashes + run_metadata_hash
        run_hash = hashlib.sha256(combined_hash.encode()).hexdigest()

        with self.get_session() as session:
            # Update runs
            stmt_run = (
                update(Run)
                .where(Run.end_time.is_(None))
                .values(end_time=end_time, hash=run_hash)
            )
            session.execute(stmt_run)

            # Update artifacts
            stmt_artifact = (
                update(Artifact)
                .where(Artifact.state == "current")
                .values(state="old")
            )
            session.execute(stmt_artifact)

        self.log(f"--------------------------- ended run {self.get_run_id()} at {end_time}")

    def get_node_id(self, node_name: str) -> int:
        with self.get_session() as session:
            node = session.query(Node).filter_by(node_name=node_name).first()
            if node is None:
                raise ValueError(f"Node name '{node_name}' does not exist in the nodes table.")
            return node.id
    
    def get_nodes_info(self, node_id: int = None, node_name: str = None) -> List[Dict]:
        with self.get_session() as session:
            stmt = select(Node)

            if node_id is not None:
                stmt = stmt.where(Node.id == node_id)
            elif node_name is not None:
                stmt = stmt.where(Node.node_name == node_name)

            result = session.execute(stmt).scalars().all()
            return [
                {
                    "id": node.id,
                    "node_name": node.node_name,
                    "node_type": node.node_type,
                    "base_type": node.base_type,
                    "init_time": node.init_time,
                }
                for node in result
            ]

    def create_entry(
        self, resource_node_name: str, filepath: str, hash: str, hash_algorithm: str, 
        state: str = "new", run_id: int = None, file_size: int = None, content_type: str = None
    ) -> None:
        node_id = self.get_node_id(resource_node_name)

        if self.entry_exists(resource_node_name, filepath):
            raise ValueError(f"Entry with location '{filepath}' already exists for node '{resource_node_name}'.")

        with self.get_session() as session:
            entry = Artifact(
                run_id=self.get_run_id() if state == "current" else run_id,
                node_id=node_id,
                location=filepath,
                created_at=datetime.now(),
                state=state,
                hash=hash,
                hash_algorithm=hash_algorithm,
                size=file_size,
                content_type=content_type
            )
            session.add(entry)
    
    def merge_artifacts_table(self, resource_node_name: str, entries: List[Dict]) -> None:
        node_id = self.get_node_id(resource_node_name)

        with self.get_session() as session:
            for entry in entries:
                if self.entry_exists(resource_node_name, entry["location"]):
                    raise ValueError(f"Entry with location '{entry['location']}' already exists for node '{resource_node_name}'.")

                new_entry = Artifact(
                    run_id=entry["run_id"],
                    node_id=node_id,
                    location=entry["location"],
                    created_at=entry["created_at"],
                    state="new",
                    hash=entry["hash"],
                    hash_algorithm=entry["hash_algorithm"],
                    size=entry["size"],
                    content_type=entry["content_type"]
                )
                session.add(new_entry)

    def entry_exists(self, resource_node_name: str, filepath: str) -> bool:
        node_id = self.get_node_id(resource_node_name)

        with self.get_session() as session:
            stmt = select(exists().where(
                Artifact.node_id == node_id,
                Artifact.location == filepath
            ))
            return session.execute(stmt).scalar()
    
    def get_num_entries(self, resource_node_name: str, state: str) -> int:
        # Validate input
        valid_states = {"new", "current", "old", "all"}
        assert state in valid_states, f"Invalid state: '{state}'. Must be one of {valid_states}"

        node_id = self.get_node_id(resource_node_name)

        with self.get_session() as session:
            query = session.query(Artifact).filter_by(node_id=node_id)
            if state != "all":
                query = query.filter_by(state=state)

            return query.count()

    #def get_entries(self, resource_node_name: str = None, location: str = None, state: str = "all") -> List[Dict]:
    def get_entries(self, resource_node_name: str = None, state: str = "all", run_id: int = None) -> List[Dict]:
        with self.get_session() as session:
            stmt = (
                select(
                    Artifact.id,
                    Artifact.run_id,
                    Artifact.location,
                    Artifact.created_at,
                    Artifact.state,
                    Artifact.hash,
                    Artifact.hash_algorithm,
                    Artifact.size,
                    Artifact.content_type,
                    Node.node_name
                )
                .join(Node, Artifact.node_id == Node.id)
            )

            if resource_node_name is not None:
                stmt = stmt.where(Node.node_name == resource_node_name)
            """
            if location is not None:
                stmt = stmt.where(Artifact.location == location)
            """
            if state != "all":
                stmt = stmt.where(Artifact.state == state)
            
            if run_id is not None:
                if run_id < 0:
                    raise ValueError("Run ID must be a positive integer.")
                stmt = stmt.where(Artifact.run_id == run_id)

            result = session.execute(stmt).all()

            return [
                {
                    "id": row.id,
                    "run_id": row.run_id,
                    "location": row.location,
                    "created_at": row.created_at,
                    "state": row.state,
                    "hash": row.hash,
                    "hash_algorithm": row.hash_algorithm,
                    "size": row.size,
                    "content_type": row.content_type,
                    "node_name": row.node_name,
                }
                for row in result
            ]
    
    def get_runs(self) -> List[Dict]:
        with self.get_session() as session:
            result = session.execute(select(Run)).scalars().all()
            return [
                {
                    "run_id": run.run_id,
                    "start_time": run.start_time,
                    "end_time": run.end_time,
                    "hash": run.hash
                }
                for run in result
            ]
    
    def log_metrics(self, node_name: str, **kwargs) -> None:
        run_id = self.get_run_id()
        node_id = self.get_node_id(node_name)

        if not kwargs:
            return  # Avoid empty inserts

        with self.get_session() as session:
            metrics = [
                Metric(run_id=run_id, node_id=node_id, metric_name=key, metric_value=value)
                for key, value in kwargs.items()
            ]
            session.add_all(metrics)

    def log_params(self, node_name: str, **kwargs) -> None:
        run_id = self.get_run_id()
        node_id = self.get_node_id(node_name)

        if not kwargs:
            return

        with self.get_session() as session:
            params = [
                Param(run_id=run_id, node_id=node_id, param_name=key, param_value=value)
                for key, value in kwargs.items()
            ]
            session.add_all(params)

    def set_tags(self, node_name: str, **kwargs) -> None:
        run_id = self.get_run_id()
        node_id = self.get_node_id(node_name)

        if not kwargs:
            return

        with self.get_session() as session:
            tags = [
                Tag(run_id=run_id, node_id=node_id, tag_name=key, tag_value=value)
                for key, value in kwargs.items()
            ]
            session.add_all(tags)
    
    def tag_artifact(self, node_name: str, location: str, **kwargs) -> None:
        run_id = self.get_run_id()
        node_id = self.get_node_id(node_name)

        if not kwargs:
            return

        with self.get_session() as session:
            artifact = session.query(Artifact).filter_by(location=location).first()
            if artifact is None:
                raise ValueError(f"Artifact with location '{location}' does not exist for node '{node_name}'.")

            tags = [
                Tag(run_id=run_id, node_id=node_id, tag_name=key, tag_value=value)
                for key, value in kwargs.items()
            ]
            artifact.tags.extend(tags)

    def get_metrics(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        with self.get_session() as session:
            stmt = (
                select(Metric.id, Metric.run_id, Metric.metric_name, Metric.metric_value, Node.node_name)
                .join(Node, Metric.node_id == Node.id)
            )

            if run_id is not None:
                stmt = stmt.where(Metric.run_id == run_id)
            if node_name is not None:
                stmt = stmt.where(Node.node_name == node_name)

            result = session.execute(stmt).all()
            return [
                {
                    "id": row.id,
                    "run_id": row.run_id,
                    "metric_name": row.metric_name,
                    "metric_value": row.metric_value,
                    "node_name": row.node_name,
                }
                for row in result
            ]
    
    def get_params(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        with self.get_session() as session:
            stmt = (
                select(
                    Param.id,
                    Param.run_id,
                    Param.param_name,
                    Param.param_value,
                    Node.node_name
                )
                .join(Node, Param.node_id == Node.id)
            )

            if run_id is not None:
                stmt = stmt.where(Param.run_id == run_id)
            if node_name is not None:
                stmt = stmt.where(Node.node_name == node_name)

            result = session.execute(stmt).all()
            return [
                {
                    "id": row.id,
                    "run_id": row.run_id,
                    "param_name": row.param_name,
                    "param_value": row.param_value,
                    "node_name": row.node_name,
                }
                for row in result
            ]
    
    def get_tags(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        with self.get_session() as session:
            stmt = (
                select(
                    Tag.id,
                    Tag.run_id,
                    Tag.tag_name,
                    Tag.tag_value,
                    Node.node_name
                )
                .join(Node, Tag.node_id == Node.id)
            )

            if run_id is not None:
                stmt = stmt.where(Tag.run_id == run_id)
            if node_name is not None:
                stmt = stmt.where(Node.node_name == node_name)

            result = session.execute(stmt).all()
            return [
                {
                    "id": row.id,
                    "run_id": row.run_id,
                    "tag_name": row.tag_name,
                    "tag_value": row.tag_value,
                    "node_name": row.node_name,
                }
                for row in result
            ]

    def log_trigger(self, node_name: str, message: str = None) -> None:
        if message is not None:
            node_id = self.get_node_id(node_name)

            with self.get_session() as session:
                trigger = Trigger(
                    node_id=node_id,
                    trigger_time=datetime.now(),
                    message=message
                )
                session.add(trigger)
    
    def get_triggers(self, node_name: str = None) -> List[Dict]:
        with self.get_session() as session:
            stmt = (
                select(
                    Trigger.id,
                    Trigger.run_triggered,
                    Trigger.trigger_time,
                    Trigger.message,
                    Node.node_name
                )
                .join(Node, Trigger.node_id == Node.id)
            )

            if node_name is not None:
                stmt = stmt.where(Node.node_name == node_name)

            result = session.execute(stmt).all()

            return [
                {
                    "id": row.id,
                    "run_triggered": row.run_triggered,
                    "trigger_time": row.trigger_time,
                    "message": row.message,
                    "node_name": row.node_name,
                }
                for row in result
            ]
    
    def get_artifact_tags(self, location: str) -> List[Dict]:
        with self.get_session() as session:
            artifact = (
                session.query(Artifact)
                .filter(Artifact.location == location)
                .first()
            )

            if not artifact:
                raise ValueError(f"Artifact with location '{location}' does not exist.")

            return [{ "id": tag.id, tag.tag_name: tag.tag_value } for tag in artifact.tags]
