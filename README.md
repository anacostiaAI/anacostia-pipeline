# Anacostia
Welcome to Anacostia. Anacostia is a framework for creating machine learning operations (MLOps) pipelines. I believe the process of creating MLOps pipelines today are too difficult; thus, this is my attempt at simplifying the entire process. 

## Notes for contributors and developers
If you are interested in contributing to Anacostia, please see CONTRIBUTORS.md. 
If you are interested in building your own plugins for Anacostia and contributing to the Anacostia ecosystem, please see DEVELOPERS.md. 

## Basic Anacostia Concepts & Terminology:
Anacostia works by allowing you to define a pipeline as a directed acyclic graph (DAG). Each node in the DAG is nothing more than a continuously running thread that does the following:
1. Waits for enough data to become available in a resource or waits for signals received from other nodes.
2. Executes a job. 
3. Sends a signal to another node upon completion of its job.

The edges of the DAG dictates which child nodes are listening for signals from which parent nodes.

There are fundamentally three types of nodes in Anacostia:
1. Metadata store nodes: stores tracking information about each time the pipeline executes (i.e., a *run*).
    - The metadata store is responsible for storing information like the start/end time of the run, metadata information about all the nodes in the pipeline, etc. 
2. Resource nodes: think of a "resource" as the inputs and outputs of your pipeline.
    - A resource can be a folder on a local filesystem, an S3 bucket, an API endpoint, a database, etc.
    - An "input resource" is a resource that is monitored for changes. When there is enough data in the input resource, it triggers the pipeline to start executing.
    - An "output resource" is a resource that is not monitored for changes. This is a resource that stores artifacts produced by the pipeline. 
    - The data in each resource resides only in that resource, it is never moved to another resource.
    - Other nodes can interact with the data in that resource via calls to its API.
3. Action nodes: executes a job in your pipeline. Examples of jobs include tasks like: data preprocessing, retraining a model on new data, evaluating a model on new data, etc.

Every node in Anacostia is inherited from these three basic nodes.

A couple things that distinguish Anacostia from other MLOps solutions:
1. Anacostia is meant to be ran locally. Of course you can run an Anacostia Pipeline in the cloud, but it is designed to be ran locally.
2. Pipelines in Anacostia can be built incrementally. Start with building the simplest pipeline possible; just one metadata store node, one input resource node, and one action node; e.g., an alerting system that monitors a resource and then sends an email notification whenever there is a certain amount of data available. From there, add in an output resource node to build something like a data preprocessing pipeline and then add in more nodes for retraining and evaluation to create a model retraining pipeline. 
3. Because every node in the Anacostia ecosystem derives from one of the three base nodes, Anacostia provides a format for a common API; thus, allowing for users to swap one node out for another node. This is great for experimentation and evaluating different solutions for your pipeline (e.g., swap out a sqlite metadata store for a redis metadata store).

## Installation
Requirements: Python 3.11 or greater, if you need to update Python, see CONTRIBUTORS.md
```
pip install anacostia-pipeline
```

## Example Usage
1. Put the following python code inside a python file.
2. Name the file simplest_test.py (there is no reason you cannot name the file something else, but for the sake of simplicity, just name it simplest_test.py).
3. Run it in terminal like so: `python simplest_test.py`.
```python
import os
import logging
import shutil
from typing import List

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode

from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.root.server import RootPipelineServer

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.sqlite.node import SqliteMetadataStoreNode



logging_tests_path = "./testing_artifacts/logging_tests"
if os.path.exists(logging_tests_path) is True:
    shutil.rmtree(logging_tests_path)
os.makedirs(logging_tests_path)

log_path = f"{logging_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)


metadata_store_path = f"{logging_tests_path}/metadata_store"
data_store_path = f"{logging_tests_path}/data_store"


class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)

class LoggingNode(BaseActionNode):
    def __init__(
        self, name: str, metadata_store: BaseMetadataStoreNode, predecessors: List[BaseNode] = None
    ) -> None:
        super().__init__(name=name, predecessors=predecessors, wait_for_connection=True)
        self.metadata_store = metadata_store
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Logging node executed", level="INFO")
        return True

metadata_store = SqliteMetadataStoreNode(name="metadata_store", uri=f"{metadata_store_path}/metadata.db")
data_store = MonitoringDataStoreNode("data_store", data_store_path, metadata_store)
logging_node = LoggingNode("logging_node", metadata_store=metadata_store, predecessors=[data_store])

pipeline = RootPipeline(nodes=[metadata_store, data_store, logging_node], loggers=logger)

if __name__ == "__main__":
    webserver = RootPipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000, logger=logger)
    webserver.run()
```
To trigger the pipeline:
1. Put the following code inside another python file.
2. Name the file create_files.py (again, there is no reason you cannot name the file something else, but for the sake of simplicity, just name it create_files.py).
3. Open up a new terminal and run the file like so: `python create_files.py`.
```python
import time

logging_tests_path = "./testing_artifacts/logging_tests"
data_store_path = f"{logging_tests_path}/data_store"

def create_file(file_path, content):
    try:
        with open(file_path, 'w') as file:
            file.write(content)
        print(f"File '{file_path}' created successfully.")
    except Exception as e:
        print(f"Error creating the file: {e}")

for i in range(10):
    create_file(f"{data_store_path}/test_file{i}.txt", f"test file {i}")
    time.sleep(1.5)
```
To view the pipeline executing:
1. Open your browser (preferably Chrome) and navigate to `http://127.0.0.1:8000`
