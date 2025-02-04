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
```python
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.sqlite.node import SqliteMetadataStoreNode

from anacostia_pipeline.pipelines.root.pipeline import RootPipeline

class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)
    
    def custom_trigger(self):
        if self.get_num_artifacts("new") >= 1:
            self.trigger()

class EmailNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        metadata_store: BaseMetadataStoreNode, 
        sender_email: str, recipient_email: str, 
        loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        self.sender_email = sender_email
        self.recipient_email = recipient_email
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        # send email to boss
        return True

if __name__ == "__main__":
    haiku_data_store = MonitoringDataStoreNode("haiku_data_store", haiku_data_store_path, metadata_store)
    email_boss_node = EmailNode("email_boss_node", predecessors=[retraining], metadata_store=metadata_store)

    metadata_store = SqliteMetadataStore(
        name="metadata_store", 
        uri=f"sqlite:///{metadata_store_path}/metadata.db"
    )

    pipeline = RootPipeline(
        nodes=[metadata_store, haiku_data_store, email_boss_node], 
        loggers=logger
    )
```
