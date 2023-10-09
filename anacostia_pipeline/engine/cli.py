from typing import List, Iterable
import time
import json
import sys
import os
import pkg_resources
from logging import Logger

import networkx as nx
from rich.console import Console
from rich.prompt import Prompt, Confirm


sys.path.append(os.path.abspath('..'))
sys.path.append(os.path.abspath('../anacostia_pipeline'))
if __name__ == "__main__":
    from base import BaseActionNode, BaseResourceNode, BaseNode
    from constants import Status
else:
    from engine.base import BaseActionNode, BaseResourceNode, BaseNode
    from engine.constants import Status



# import IPython
class InvalidNodeDependencyError(Exception):
    pass


class Pipeline:
    """
    Pipeline is a class that is in charge of graph management and execution; this includes:
    1. Providing an API to interact with the nodes.
        - Pipeline class will be used to create a CLI and a browser GUI.
        - CLI commands include help, version, start, shutdown, pause, resume, and check_status. More commands will be added later.
        - Browser GUI will be added later.
    2. Saving graph as graph.json file and loading a graph.json file back into the pipeline to recreate the DAG. 
    2. Ensuring the user built the graph correctly (i.e., ensuring the graph is a DAG)
    """

    def __init__(self, nodes: Iterable[BaseNode], logger: Logger = None) -> None:
        self.graph = nx.DiGraph()

        # Add nodes into graph
        for node in nodes:
            self.graph.add_node(node)

        # Add edges into graph
        for node in nodes:
            for predecessor in node.predecessors:
                self.graph.add_edge(predecessor, node)
        
        # set successors for all nodes
        for node in nodes:
            node.successors = list(self.graph.successors(node))
        
        # Set logger for all nodes
        if logger is not None:
            for node in nodes:
                node.set_logger(logger)

        # check if graph is acyclic (i.e., check if graph is a DAG)
        if not nx.is_directed_acyclic_graph(self.graph):
            raise InvalidNodeDependencyError("Node Dependencies do not form a Directed Acyclic Graph")

        self.nodes: List[BaseNode] = list(nx.topological_sort(self.graph))
        
    def launch_nodes(self):
        """
        Lanches all the registered nodes
        """
        for node in self.nodes:
            # Note: since node is a subclass of Thread, calling start() will run the run() method
            node.start()

    def terminate_nodes(self) -> None:
        # terminating nodes need to be done in reverse order so that the successor nodes are terminated before the predecessor nodes
        # this is because the successor nodes will continue to listen for signals from the predecessor nodes,
        # and if the predecessor nodes are terminated first, then the sucessor nodes will never receive the signals,
        # thus, the successor nodes will never be terminated.
        # predecessor nodes need to wait for the successor nodes to terminate before they can terminate. 

        print("Terminating nodes")
        for node in reversed(self.nodes):
            node.stop()
            node.join()
        print("All nodes terminated")


class DataStoreNode(BaseResourceNode):
    def __init__(self, name: str, uri: str) -> None:
        super().__init__(name, uri)
    
    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")
    
    def update_state(self):
        print(f"Updating state of {self.name}")
        time.sleep(1)
        print(f"Finished updating state of {self.name}")

class FeatureStoreNode(BaseResourceNode):
    def __init__(self, name: str, uri: str) -> None:
        super().__init__(name, uri)

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def update_state(self):
        print(f"Updating state of {self.name}")
        time.sleep(1)
        print(f"Finished updating state of {self.name}")

class ArtifactStoreNode(BaseResourceNode):
    def __init__(self, name: str, uri: str) -> None:
        super().__init__(name, uri)

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def update_state(self):
        print(f"Updating state of {self.name}")
        time.sleep(1)
        print(f"Finished updating state of {self.name}")

class ModelRegistryNode(BaseResourceNode):
    def __init__(self, name: str, uri: str) -> None:
        super().__init__(name, uri)

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def update_state(self):
        print(f"Updating state of {self.name}")
        time.sleep(1)
        print(f"Finished updating state of {self.name}")

class AndNode(BaseActionNode):
    def __init__(self, name: str, predecessors: List[BaseNode]) -> None:
        super().__init__(name, predecessors)
    
    def execute(self):
        print(f"Executing {self.name}")
        time.sleep(1)
        print(f"Finished executing {self.name}")
        return True

class DataPrepNode(BaseActionNode):
    def __init__(
        self, 
        name: str, 
        data_store: DataStoreNode, 
        feature_store: FeatureStoreNode, 
        and_node: AndNode
    ) -> None:
        super().__init__(name=name, predecessors=[data_store, feature_store, and_node])

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def execute(self):
        print(f"Executing {self.name}")
        time.sleep(1)
        print(f"Finished executing {self.name}")
        return True

class RetrainingNode(BaseActionNode):
    def __init__(
        self, 
        name: str, 
        feature_store: FeatureStoreNode, 
        model_registry: ModelRegistryNode, 
        data_prep: DataPrepNode,
        and_node: AndNode
    ) -> None:
        super().__init__(name, predecessors=[feature_store, data_prep, model_registry, and_node])

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def execute(self):
        print(f"Executing {self.name}")
        time.sleep(1)
        print(f"Finished executing {self.name}")
        return True

class ModelEvaluationNode(BaseActionNode):
    def __init__(self, name: str, model_registry: ModelRegistryNode, and_node: AndNode) -> None:
        super().__init__(name, predecessors=[model_registry, and_node])
    
    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")
    
    def execute(self):
        print(f"Executing {self.name}")
        time.sleep(1)
        print(f"Finished executing {self.name}")
        return True

class ReportingNode(BaseActionNode):
    def __init__(self, name: str, artifact_store: ArtifactStoreNode, retraining_node: RetrainingNode, and_node: AndNode) -> None:
        super().__init__(name, predecessors=[artifact_store, retraining_node, and_node])

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def execute(self):
        print(f"Executing {self.name}")
        time.sleep(1)
        print(f"Finished executing {self.name}")
        return True


if __name__ == "__main__":
    data_store_node = DataStoreNode("data store", "http://localhost:8080")
    feature_store_node = FeatureStoreNode("feature store", "http://localhost:8080")
    artifact_store_node = ArtifactStoreNode("artifact store", "http://localhost:8080")
    model_registry_node = ModelRegistryNode("model registry", "http://localhost:8080")
    and_node1 = AndNode("AND node 1", [data_store_node, feature_store_node, artifact_store_node, model_registry_node])
    #and_node2 = AndNode("AND node 2", [and_node1])
    #and_node3 = AndNode("AND node 3", [and_node1])
    data_prep_node = DataPrepNode("data prep", data_store_node, feature_store_node, and_node1)
    retraining_node = RetrainingNode("retraining", feature_store_node, model_registry_node, data_prep_node, and_node1)
    reporting_node = ReportingNode("reporting", artifact_store_node, retraining_node, and_node1)
    model_evaluation_node = ModelEvaluationNode("model evaluation", model_registry_node, and_node1)

    pipeline = Pipeline(
        [
            feature_store_node, artifact_store_node, model_registry_node, data_store_node,  
            retraining_node, data_prep_node, reporting_node, model_evaluation_node, 
            and_node1, #and_node2, and_node3
        ]
    )

    #print(pipeline.nodes)
    #print(retraining_node.predecessors)
    #print(model_registry_node.successors)

    pipeline.launch_nodes()
    time.sleep(20)
    pipeline.terminate_nodes()