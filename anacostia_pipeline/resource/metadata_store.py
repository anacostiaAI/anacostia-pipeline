import sys
import os
import json
from datetime import datetime
from typing import List, Dict

sys.path.append("../../anacostia_pipeline")
from engine.node import ResourceNode


class MetadataStoreNode(ResourceNode):
    def __init__(self, name: str, metadata_store_path: str, init_state: str = "old", **kwargs) -> None:
        self.metadata_store_path = metadata_store_path
        self.metadata_store_path_json_path = os.path.join(self.metadata_store_path, "metadata_store.json")

        assert init_state in ["current", "old"], f"init_state argument of DataStoreNode must be either 'current' or 'old', not '{init_state}'."
        metadata = dict(kwargs)
        metadata["state"] = init_state
        metadata["created_at"] = str(datetime.now())
        self.metadata = metadata

        super().__init__(name, "metadata_store")
    
    @ResourceNode.resource_accessor
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")

        if os.path.exists(self.metadata_store_path) is False:
            os.makedirs(self.metadata_store_path, exist_ok=True)

        if os.path.exists(self.metadata_store_path_json_path) is False:
            with open(self.metadata_store_path_json_path, 'w') as json_file:
                json_entry = {
                    "node": self.name,
                    "resource_path": self.metadata_store_path,
                    "entries": []
                }
                
                json.dump(json_entry, json_file, indent=4)
        
        self.log(f"Node '{self.name}' setup complete.")

        # TODO: implement a function that installs the database, spin up the database,
        # and open a connection to the database when the node is started
        
        # Note: the metadata store is always empty when the node is started
    
    def pre_trigger(self) -> bool:
        if self.trigger_condition() is True:
            self.trigger()
            return True
        else:
            return False

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def insert_metadata(self, **kwargs):

        with open(self.metadata_store_path_json_path, 'r') as json_file:
            json_data = json.load(json_file)
            metadata = {}
            metadata["index"] = len(json_data["entries"])
            metadata.update(kwargs)
            metadata["state"] = "new"
            metadata["created_at"] = str(datetime.now())
            json_data["entries"].append(metadata)
        
        self.log(f"Inserting metadata: {metadata}")
        
        with open(self.metadata_store_path_json_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def trigger_condition(self) -> bool:
        # in the default implementation, we trigger the next node as soon as we see a new model in the registry.
        return True

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def get_metadata(self, state: str) -> List[Dict]:
        with open(self.metadata_store_path_json_path, 'r') as json_file:
            json_data = json.load(json_file)
            return [entry for entry in json_data["entries"] if entry["state"] == state]
    
    @ResourceNode.await_references
    @ResourceNode.resource_accessor
    def execute(self):
        with open(self.metadata_store_path_json_path, 'r') as json_file:
            json_data = json.load(json_file)

            for entry in json_data["entries"]:
                if entry["state"] == "current":
                    entry["state"] = "old"
                    self.log(f"current -> old: {entry}")
                    entry["updated_at"] = str(datetime.now())

            for entry in json_data["entries"]:
                if entry["state"] == "new":
                    entry["state"] = "current"
                    self.log(f"new -> current: {entry}")
                    entry["updated_at"] = str(datetime.now())

        with open(self.metadata_store_path_json_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)

        return True
    
    def on_exit(self):
        self.log(f"exiting node {self.name}")


if __name__ == "__main__":
    metadata_store = MetadataStoreNode("metadata_store", "../../tests/testing_artifacts/metadata_store")
    metadata_store.setup()
    metadata_store.insert_metadata(accuracy=0.98, auc=0.977)
    print(metadata_store.get_metadata("new"))
