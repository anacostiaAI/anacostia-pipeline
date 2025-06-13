from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from datetime import datetime

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode



class BaseNoSQLMetadataStoreNode(BaseMetadataStoreNode, ABC):
    """
    Abstract base class for NoSQL metadata store nodes.
    Subclasses must implement NoSQL-backed versions of metadata logging and query methods.
    """

    @abstractmethod
    def insert(self, doc: Dict) -> None:
        pass

    @abstractmethod
    def query(self, filters: Dict) -> List[Dict]:
        pass

    @abstractmethod
    def update(self, filters: Dict, updates: Dict) -> int:
        pass

    # ---- Required Implementations from BaseMetadataStoreNode ---- #

    def add_node(self, node_name: str, node_type: str, base_type: str) -> None:
        self.insert({
            "type": "node",
            "node_name": node_name,
            "node_type": node_type,
            "base_type": base_type,
            "init_time": datetime.utcnow()
        })

    def get_nodes_info(self, node_id: int = None, node_name: str = None) -> List[Dict]:
        filters = {"type": "node"}
        if node_id is not None:
            filters["id"] = node_id
        if node_name is not None:
            filters["node_name"] = node_name
        return self.query(filters)

    def create_entry(self, resource_node_name: str, **kwargs) -> None:
        kwargs.update({
            "type": "artifact",
            "node_name": resource_node_name,
            "created_at": datetime.utcnow()
        })
        self.insert(kwargs)

    def get_entries(self, resource_node_name: str, state: str) -> List[Dict]:
        filters = {"type": "artifact", "state": state}
        if resource_node_name:
            filters["node_name"] = resource_node_name
        return self.query(filters)

    def update_entry(self, resource_node_name: str, entry_id: int, **kwargs) -> None:
        filters = {"type": "artifact", "id": entry_id, "node_name": resource_node_name}
        self.update(filters, kwargs)

    def get_num_entries(self, resource_node_name: str, state: str) -> int:
        filters = {"type": "artifact", "state": state}
        if resource_node_name:
            filters["node_name"] = resource_node_name
        return len(self.query(filters))

    def entry_exists(self, resource_node_name: str) -> bool:
        filters = {"type": "artifact", "node_name": resource_node_name}
        return len(self.query(filters)) > 0

    def log_metrics(self, node_name: str, **kwargs) -> None:
        run_id = self.get_run_id()
        for metric_name, metric_value in kwargs.items():
            self.insert({
                "type": "metric",
                "run_id": run_id,
                "node_name": node_name,
                "metric_name": metric_name,
                "metric_value": metric_value
            })

    def log_params(self, node_name: str, **kwargs) -> None:
        run_id = self.get_run_id()
        for param_name, param_value in kwargs.items():
            self.insert({
                "type": "param",
                "run_id": run_id,
                "node_name": node_name,
                "param_name": param_name,
                "param_value": param_value
            })

    def set_tags(self, node_name: str, **kwargs) -> None:
        run_id = self.get_run_id()
        for tag_name, tag_value in kwargs.items():
            self.insert({
                "type": "tag",
                "run_id": run_id,
                "node_name": node_name,
                "tag_name": tag_name,
                "tag_value": tag_value
            })

    def get_metrics(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        filters = {"type": "metric"}
        if node_name: filters["node_name"] = node_name
        if run_id is not None: filters["run_id"] = run_id
        return self.query(filters)

    def get_params(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        filters = {"type": "param"}
        if node_name: filters["node_name"] = node_name
        if run_id is not None: filters["run_id"] = run_id
        return self.query(filters)

    def get_tags(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        filters = {"type": "tag"}
        if node_name: filters["node_name"] = node_name
        if run_id is not None: filters["run_id"] = run_id
        return self.query(filters)

    def start_run(self) -> None:
        run_id = self.get_run_id()
        start_time = datetime.utcnow()

        self.insert({
            "type": "run",
            "run_id": run_id,
            "start_time": start_time
        })

        self.update(
            {"type": "artifact", "run_id": None, "state": "new"},
            {"run_id": run_id, "state": "current"}
        )

        self.update(
            {"type": "trigger", "run_triggered": None, "trigger_time": ("$lt", start_time)},
            {"run_triggered": run_id}
        )

        self.log(f"--------------------------- started run {run_id} at {start_time}")

    def end_run(self) -> None:
        end_time = datetime.utcnow()

        self.update(
            {"type": "run", "end_time": None},
            {"end_time": end_time}
        )

        self.update(
            {"type": "artifact", "end_time": None, "state": "current"},
            {"end_time": end_time, "state": "old"}
        )

        self.log(f"--------------------------- ended run {self.get_run_id()} at {end_time}")

    def log_trigger(self, node_name: str, message: str = None) -> None:
        if message:
            self.insert({
                "type": "trigger",
                "node_name": node_name,
                "trigger_time": datetime.utcnow(),
                "message": message
            })
