import os
import json
from datetime import datetime
from logging import Logger
from typing import List, Union

from ..engine.base import BaseMetadataStoreNode, BaseResourceNode

class JsonMetadataStoreNode(BaseMetadataStoreNode):
    def __init__(self, name: str, tracker_dir: str, loggers: Union[Logger, List[Logger]] = None) -> None:
        super().__init__(
            name,
            uri=os.path.join(tracker_dir, f"{name}.json"),
            loggers=loggers
        )
        self.tracker_dir = tracker_dir

    def resource_uri(self, r_node: BaseResourceNode):
        return os.path.join(self.tracker_dir, f"{r_node.name}.json")

    @BaseMetadataStoreNode.metadata_accessor
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")

        if os.path.exists(self.tracker_dir) is False:
            os.makedirs(self.tracker_dir, exist_ok=True)

        if os.path.exists(self.uri) is False:
            with open(self.uri, "w") as json_file:
                json_entry = {
                    "node": self.name,
                    "path": self.uri,
                    "node initialization time:": str(datetime.now()),
                    "runs": []
                }

                json.dump(json_entry, json_file, indent=4)
                json_file.flush()
                self.log(f"Created metadata store file at {self.uri}")

        self.log(f"Node '{self.name}' setup complete.")
    
    @BaseMetadataStoreNode.metadata_accessor
    def create_resource_tracker(self, resource_node: BaseResourceNode) -> None:
        artifact_tracker_filepath = self.resource_uri(resource_node)

        if os.path.exists(artifact_tracker_filepath) is False:
            with open(artifact_tracker_filepath, "w") as json_file:
                json_entry = {
                    "node": resource_node.name,
                    "path": artifact_tracker_filepath,
                    "node initialization time:": str(datetime.now()),
                    "samples": []
                }

                json.dump(json_entry, json_file, indent=4)
                json_file.flush()
                self.log(f"Created artifact tracker file at {artifact_tracker_filepath}")

    @BaseMetadataStoreNode.metadata_accessor
    def create_entry(self, resource_node: BaseResourceNode, **kwargs) -> None:
        artifact_tracker_filepath = self.resource_uri(resource_node)

        with open(artifact_tracker_filepath, "r") as json_file:
            json_data = json.load(json_file)

        artifact_entry = {key: value for key, value in kwargs.items()}
        artifact_entry["created_at"] = str(datetime.now())
        artifact_entry["entry_id"] = self.get_num_entries(resource_node, "all")
        json_data["samples"].append(artifact_entry)

        with open(artifact_tracker_filepath, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
            json_file.flush()

    def get_num_entries(self, resource_node: BaseResourceNode, state: str) -> int:
        return len(self.get_entries(resource_node, state))
    
    def get_entries(self, resource_node: BaseResourceNode, state: str) -> List[dict]:
        if state not in ("new", "current", "old", "all"):
            raise ValueError(f"state argument of get_samples must be either 'new', 'current', 'old', or 'all', not '{state}'.")

        artifact_tracker_filepath = self.resource_uri(resource_node)

        with open(artifact_tracker_filepath, "r") as json_file:
            json_data = json.load(json_file)

        artifacts = []
        for file_entry in json_data["samples"]:
            if state == "all":
                artifacts.append(file_entry)

            elif state == "new":
                if "run_id" not in file_entry.keys():
                    artifacts.append(file_entry)
            
            elif state == "current":
                if ("run_id" in file_entry.keys()) and ("end_time" not in file_entry.keys()):
                    artifacts.append(file_entry)
            
            elif state == "old":
                if ("run_id" in file_entry.keys()) and ("end_time" in file_entry.keys()):
                    artifacts.append(file_entry)
        
        return artifacts
    
    def update_entry(self, resource_node: BaseResourceNode, entry_id: int, **kwargs) -> None:
        artifact_tracker_filepath = self.resource_uri(resource_node)

        with open(artifact_tracker_filepath, "r") as json_file:
            json_data = json.load(json_file)

        for sample in json_data["samples"]:
            if sample["entry_id"] == entry_id:
                for key, value in kwargs.items():
                    sample[key] = value
        
        with open(artifact_tracker_filepath, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
            json_file.flush()

    @BaseMetadataStoreNode.metadata_accessor
    def get_run(self, run_id: int) -> int:
        with open(self.uri, "r") as json_file:
            json_data = json.load(json_file)
        
        for run in json_data["runs"]:
            if run["run_id"] == run_id:
                return run
        
        return None
    
    @BaseMetadataStoreNode.metadata_accessor
    def start_run(self) -> None:
        # update the tracker file, start the run by adding a 'start time'
        with open(self.uri, "r") as json_file:
            json_data = json.load(json_file)

        run_entry = {
            "run_id": self.run_id,
            "start_time": str(datetime.now()),
            "metrics": {},
            "params": {},
            "tags": {},
        }

        json_data["runs"].append(run_entry)

        with open(self.uri, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
            json_file.flush()

        self.log(f"--------------------------- started run {self.run_id} at {datetime.now()}")
    
    @BaseMetadataStoreNode.metadata_accessor
    def log_metrics(self, **kwargs) -> None:
        with open(self.uri, "r") as json_file:
            json_data = json.load(json_file)
        
        for run in json_data["runs"]:
            if run["run_id"] == self.run_id:
                for key, value in kwargs.items(): 
                    run["metrics"][key] = value

        with open(self.uri, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
            json_file.flush()

    @BaseMetadataStoreNode.metadata_accessor
    def log_params(self, **kwargs) -> None:
        with open(self.uri, "r") as json_file:
            json_data = json.load(json_file)
        
        for run in json_data["runs"]:
            if run["run_id"] == self.run_id:
                for key, value in kwargs.items(): 
                    run["params"][key] = value

        with open(self.uri, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
            json_file.flush()

    @BaseMetadataStoreNode.metadata_accessor
    def set_tags(self, **kwargs) -> None:
        with open(self.uri, "r") as json_file:
            json_data = json.load(json_file)
        
        for run in json_data["runs"]:
            if run["run_id"] == self.run_id:
                for key, value in kwargs.items(): 
                    run["tags"][key] = value

        with open(self.uri, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
            json_file.flush()

    @BaseMetadataStoreNode.metadata_accessor
    def add_run_id(self) -> None:
        # update the run_id for all new entries for all resource nodes
        for successor in self.successors:
            if isinstance(successor, BaseResourceNode):
                new_entries = self.get_entries(successor, "new")
                for entry in new_entries:
                    self.update_entry(successor, entry["entry_id"], state="current", run_id=self.run_id)

    @BaseMetadataStoreNode.metadata_accessor
    def add_end_time(self) -> None:
        # update the run_id for all new entries for all resource nodes
        for successor in self.successors:
            if isinstance(successor, BaseResourceNode):
                new_entries = self.get_entries(successor, "current")
                for entry in new_entries:
                    self.update_entry(successor, entry["entry_id"], state="old", end_time=str(datetime.now()))
    
    @BaseMetadataStoreNode.metadata_accessor
    def end_run(self) -> None:
        # update the tracker file, end the run by adding an 'end_time', and increment the run_id
        with open(self.uri, "r") as json_file:
            json_data = json.load(json_file)
        
        for run in json_data["runs"]:
            if run["run_id"] == self.run_id: 
                run["end_time"] = str(datetime.now())
        
        with open(self.uri, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
            json_file.flush()

        self.log(f"--------------------------- ended run {self.run_id} at {datetime.now()}")
