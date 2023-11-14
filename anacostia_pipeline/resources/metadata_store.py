import sys
import os
import json
from datetime import datetime
from logging import Logger

sys.path.append("../../anacostia_pipeline")
from engine.base import BaseMetadataStoreNode



class JsonMetadataStoreNode(BaseMetadataStoreNode):
    def __init__(self, name: str, tracker_filename: str, logger: Logger = None) -> None:
        super().__init__(name, tracker_filename, logger)
        
    @BaseMetadataStoreNode.metadata_accessor
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")
        self.tracker_filepath = os.path.join(self.anacostia_path, self.tracker_filename)

        if os.path.exists(self.tracker_filepath) is False:
            with open(self.tracker_filepath, "w") as json_file:
                json_entry = {
                    "node": self.name,
                    "path": self.tracker_filepath,
                    "node initialization time:": str(datetime.now()),
                    "runs": []
                }

                json.dump(json_entry, json_file, indent=4)
                json_file.flush()
                self.log(f"Created metadata store file at {self.tracker_filepath}")

        self.log(f"Node '{self.name}' setup complete.")
    
    @BaseMetadataStoreNode.metadata_accessor
    def create_run(self) -> None:
        with open(self.tracker_filepath, "r") as json_file:
            json_data = json.load(json_file)

        run_entry = {
            "run id": self.run_id,
            "start time": str(datetime.now()),
            "metrics": {},
            "params": {},
            "tags": {},
            "end time": None
        }

        json_data["runs"].append(run_entry)

        with open(self.tracker_filepath, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
            json_file.flush()

        self.log(f"--------------------------- started run {self.run_id} at {datetime.now()}")
    
    @BaseMetadataStoreNode.metadata_accessor
    def get_run(self, run_id: int) -> int:
        with open(self.tracker_filepath, "r") as json_file:
            json_data = json.load(json_file)
        
        for run in json_data["runs"]:
            if run["run id"] == run_id:
                return run
        
        return None
    
    @BaseMetadataStoreNode.metadata_accessor
    def end_run(self) -> None:
        with open(self.tracker_filepath, "r") as json_file:
            json_data = json.load(json_file)
        
        for run in json_data["runs"]:
            if run["run id"] == self.run_id: 
                run["end time"] = str(datetime.now())

        with open(self.tracker_filepath, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
            json_file.flush()

        self.log(f"--------------------------- ended run {self.run_id} at {datetime.now()}")
        self.run_id += 1