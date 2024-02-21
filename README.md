# Anacostia
Welcome to Anacostia. Anacostia is a framework for creating machine learning operations (MLOps) pipelines. I believe the process of creating MLOps pipelines today are too difficult; thus, this is my attempt at simplifying the entire process. 

## Notes for contributors and developers
If you are interested in contributing to Anacostia, please see CONTRIBUTORS.md. 
If you are interested in building your own plugins for Anacostia and contributing to the Anacostia ecosystem, please see DEVELOPERS.md. 

## Basic Anacostia Concepts & Terminology:
Anacostia works by allowing you to define a pipeline as a directed acyclic graph (DAG). Each node in the DAG is nothing more than a continuously running thread that does the following:
1. Waits for enough data to become available in a resource or waits for signals recieved from other nodes.
2. Executes a job. 
3. Send signal to another node upon completion of its job.

The edges of the DAG dictates which child nodes are listening for signals from which parent nodes.

There are fundamentally three types of nodes in Anacostia:
1. Metadata store nodes: stores tracking information about each time the pipeline executes (i.e., a *run*).
    - The metadata store is responsibles for storing information like the start/end time of the run, metadata information about all the nodes in the pipeline, etc. 
    - All metadata store nodes must implement the following methods: ...
2. Resource nodes: think of a "resource" as the inputs and outputs of your pipeline.
    - A resource can be a folder on a local filesystem, an S3 bucket, an API endpoint, a database, etc.
    - An "input resource" is a resource that is monitored for changes. When there is enough data in the input resource, it triggers the pipeline to start executing.
    - An "output resource" is a resource that is not monitored for changes. This is a resource that stores artifacts produced by the pipeline. 
    - The data in each resource resides only in that resource, it is never moved to another resource.
    - Other nodes can interact with the data in that resource via calls to its API.
    - All resource nodes must implement the following methods: ...
3. Action nodes: executes a job in your pipeline. Examples of jobs include tasks like: data preprocessing, retraining a model on new data, evaluating a model on new data, etc.
    - All action nodes must implement the following methods: ...

Every node in Anacostia is inherited from these three basic nodes.

A couple things that distinguish Anacostia from other MLOps solutions:
1. Anacostis is meant to be ran locally. Of course you can run an Anacostia Pipeline in the cloud, but it is designed to be ran locally.
2. Pipelines in Anacostia can be built incrementally. Start with building the simplest pipeline possible; just one metadata store node, one input resource node, and one action node; e.g., an alerting system that monitors a resource and then sends an email notification whenever there is a certain amount of data available. From there, add in an output resource node to build something like a data preprocessing pipeline and then add in more nodes for retraining and evaluation to create a model retraining pipeline. 
3. Because every node in the Anacostia ecosystem derives from one of the three base nodes, Anacostia provides a format for a common API; thus, allowing for users to swap one node out for another node. This is great for experimentation and evaluating different solutions for your pipeline (e.g., swap out a sqlite metadata store for a redis metadata store).

## Installation
```
pip install anacostia-pipeline[web]
```

## Example Usage
```python
from anacostia_pipeline.engine.base import BaseNode, BaseActionNode, BaseMetadataStoreNode
from anacostia_pipeline.engine.pipeline import Pipeline
from anacostia_pipeline.dashboard.webserver import run_background_webserver

from anacostia_pipeline.resources.filesystem_store import FilesystemStoreNode
from anacostia_pipeline.metadata.sql_metadata_store import SqliteMetadataStore

class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)
    
    def trigger_condition(self) -> bool:
        num_new = self.get_num_artifacts("new")
        return num_new >= 1

class ShakespeareEvalNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        metadata_store: BaseMetadataStoreNode, loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        self.metadata_store.log_metrics(shakespeare_test_loss=1.47)
        return True

if __name__ == "__main__":
    haiku_data_store = MonitoringDataStoreNode("haiku_data_store", haiku_data_store_path, metadata_store)
    shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", predecessors=[retraining], metadata_store=metadata_store)

    metadata_store = SqliteMetadataStore(
        name="metadata_store", 
        uri=f"sqlite:///{metadata_store_path}/metadata.db"
    )

    pipeline = Pipeline(
        nodes=[metadata_store, haiku_data_store, shakespeare_eval], 
        loggers=logger
    )
```