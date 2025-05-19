# Anacostia
Welcome to Anacostia. Anacostia is a framework for creating machine learning operations (MLOps) pipelines. I believe the process of creating MLOps pipelines today are too difficult; thus, this is my attempt at simplifying the entire process. 

## Notes for contributors and developers
If you are interested in contributing to Anacostia, please see CONTRIBUTORS.md. 
If you are interested in building your own plugins for Anacostia and contributing to the Anacostia ecosystem, please see DEVELOPERS.md. 

## Basic Anacostia Concepts & Terminology:
Anacostia works by allowing you to define a pipeline as a directed acyclic graph (DAG). Each node in the DAG is nothing more than a continuously running thread that does the following:
1. Waits for some event to happen (e.g., if there is enough data to become available in a resource, if a metric breaks through a certain threshold, etc) or waits for signals from other nodes.
2. Executes a job. 
3. Sends a signal to another node upon completion of its job.

The edges of the DAG dictates which child nodes are listening for signals from which parent nodes.

There are fundamentally three types of nodes in Anacostia:
1. Metadata store nodes: stores tracking information about each time the pipeline executes (i.e., a *run*).
    - The metadata store is responsible for storing information like the start/end time of the run, metadata information about all the nodes in the pipeline, etc.
    - The metadata store can also act as a way to monitor metrics and trigger the pipeline when a metric breaks passes through a threshold. 
2. Resource nodes: think of a "resource" as the inputs and outputs of your pipeline.
    - A resource can be a folder on a local filesystem, an S3 bucket, an API endpoint, a database, etc.
    - An "input resource" is a resource that is monitored for changes. When there is enough data in the input resource, it triggers the pipeline to start executing.
    - An "output resource" is a resource that is not monitored for changes. This is a resource that stores artifacts produced by the pipeline. 
    - The data in each resource resides only in that resource, it is never moved to another resource.
    - Other nodes can interact with the data in that resource via calls to its API.
3. Action nodes: executes a job in your pipeline. Examples of jobs include tasks like: data preprocessing, retraining a model on new data, evaluating a model on new data, logging, asking for approval, etc.

Every node in Anacostia is inherited from these three basic nodes.

A couple things that distinguish Anacostia from other MLOps solutions:
1. Anacostia is meant to be ran locally. Of course you can run an Anacostia pipeline in the cloud, but it is designed to be ran locally.
2. Pipelines in Anacostia can be built incrementally. Start with building the simplest pipeline possible; just one metadata store node, one input resource node, and one action node; e.g., an alerting system that monitors a resource and then sends an email notification whenever there is a certain amount of data available. From there, add in an output resource node to build something like a data preprocessing pipeline, and then add in more nodes for retraining and evaluation to create a model retraining pipeline. 
3. Because every node in the Anacostia ecosystem derives from one of the three base nodes, Anacostia provides a format for a common API; thus, allowing for users to swap one node out for another node. This is great for experimentation and evaluating different solutions for your pipeline (e.g., swap out a sqlite metadata store for a redis metadata store).

## Installation
Requirements: Python 3.11 or greater, if you need to update Python, see CONTRIBUTORS.md
```
pip install anacostia-pipeline
```

## Example Usage
1. Put the following python code inside a python file.
2. Name the file simplest_test.py (there is no reason you cannot name the file something else, but for the sake of simplicity, just name it `test.py`).
3. Run it in terminal like so: `python test.py`.
```python
import os
import shutil
from typing import List

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer

# Create the testing artifacts directory for the SQLAlchemy tests
tests_path = "./testing_artifacts"
if os.path.exists(tests_path) is True:
    shutil.rmtree(tests_path)
os.makedirs(tests_path)
metadata_store_path = f"{tests_path}/metadata_store"
data_store_path = f"{tests_path}/data_store"

# override the BaseActionNode to create a custom action node. This is just a placeholder for the actual implementation
class PrintingNode(BaseActionNode):
    def __init__(self, name: str, predecessors: List[BaseNode] = None) -> None:
        super().__init__(name=name, predecessors=predecessors)
    
    async def execute(self, *args, **kwargs) -> bool:
        print("Logging node executed")
        return True

# Create the nodes
metadata_store = SQLiteMetadataStoreNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db")
data_store = FilesystemStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store)
printing_node = PrintingNode("logging_node", predecessors=[data_store])

# Create the pipeline
pipeline = Pipeline(nodes=[metadata_store, data_store, printing_node])

# Create the web server
webserver = PipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000)
webserver.run()
```
To trigger the pipeline:
1. Put the following code inside another python file.
2. Name the file `create_files.py` (again, there is no reason you cannot name the file something else, but for the sake of simplicity, just name it `create_files.py`).
3. Open up a new terminal and run the file like so: `python create_files.py`.
```python
import time

tests_path = "./testing_artifacts"
data_store_path = f"{tests_path}/data_store"

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
2. Click on the nodes to see the UI specific for that node