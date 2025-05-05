from typing import List, Iterable, Union
from threading import Thread
from datetime import datetime
from logging import Logger
from pydantic import BaseModel, ConfigDict

import networkx as nx

from anacostia_pipeline.nodes.node import BaseNode, NodeModel
from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.utils.constants import Status



class PipelineModel(BaseModel):
    '''
    A Pydantic Model for validation and serialization of a Pipeline
    '''
    model_config = ConfigDict(from_attributes=True)

    nodes: List[NodeModel]



class LeafPipeline:
    """
    Pipeline is a class that is in charge of graph management and execution; this includes:
    1. Providing an API to interact with the nodes.
        - Pipeline class will be used to create a CLI and a browser GUI.
        - CLI commands include help, version, start, shutdown, pause, resume, and check_status. More commands will be added later.
        - Browser GUI will be added later.
    2. Saving graph as graph.json file and loading a graph.json file back into the pipeline to recreate the DAG. 
    2. Ensuring the user built the graph correctly (i.e., ensuring the graph is a DAG)
    """

    def __init__(self, name: str, nodes: Iterable[BaseNode], loggers: Union[Logger, List[Logger]] = None) -> None:

        self.node_dict = dict()
        self.graph = nx.DiGraph()
        self.name = name

        # Add nodes into graph
        for node in nodes:
            self.graph.add_node(node)
            self.node_dict[node.name] = node

        # Add edges into graph
        for node in nodes:
            for predecessor in node.predecessors:
                self.graph.add_edge(predecessor, node)
        
        # Set logger for all nodes
        if loggers is not None:
            for node in nodes:
                node.add_loggers(loggers)

        """
        # check 1: make sure graph is acyclic (i.e., check if graph is a DAG)
        if not nx.is_directed_acyclic_graph(self.graph):
            raise InvalidNodeDependencyError("Node Dependencies do not form a Directed Acyclic Graph")
        """
        
        self.nodes: List[BaseNode] = list(nx.topological_sort(self.graph))

        """
        # check 2: make sure graph is not disconnected
        if not nx.is_weakly_connected(self.graph):
            raise InvalidNodeDependencyError(f"Node '{node.name}' is disconnected from graph")

        # check 3: make sure root node is a metadata store node
        if isinstance(self.nodes[0], BaseMetadataStoreNode) is not True:
            raise InvalidNodeDependencyError(f"Root node \'{self.nodes[0].name}\' must be a metadata store node. Got: {type(self.nodes[0]).__name__}")
        
        # check 4: make sure there is only one metadata store node
        for node in self.nodes:
            if (node != self.nodes[0]) and (isinstance(nodes, BaseMetadataStoreNode) is True):
                raise InvalidNodeDependencyError("There can only be one metadata store node")
        """
                
        """
        # check 5: make sure all resource nodes are successors of the metadata store node
        for node in self.nodes:
            if isinstance(node, BaseResourceNode) is True:
                if node not in list(self.graph.successors(self.metadata_store)):
                    raise InvalidNodeDependencyError("All resource nodes must be successors of the metadata store node")

        # check 6: make sure all resource nodes have at least one successor
        for node in self.nodes:
            if isinstance(node, BaseResourceNode) is True:
                if len(list(self.graph.successors(node))) == 0:
                    raise InvalidNodeDependencyError("All resource nodes must have at least one successor")

        # check 7: make sure all successors of a resource node are action nodes 
        for node in self.nodes:
            if isinstance(node, BaseResourceNode) is True:
                for successor in list(self.graph.successors(node)):
                    if (isinstance(successor, BaseMetadataStoreNode) is True) or (isinstance(successor, BaseResourceNode) is True):
                        raise InvalidNodeDependencyError("All successors of a resource node must be action nodes")
        """

    def __getitem__(self, key):
        return self.node_dict.get(key, None)

    def model(self):
        return PipelineModel(nodes=[n.model() for n in self.nodes])
    
    def setup_nodes(self):
        """
        Sets up all the registered nodes in topological order.
        """

        def __setup_nodes(nodes: List[BaseNode]):
            """
            Sets up all the nodes in the pipeline.
            """

            threads: List[Thread] = []
            for node in nodes:
                node.log(f"--------------------------- started setup phase of {node.name} at {datetime.now()}", level="INFO")
                thread = Thread(target=node.leaf_setup)
                node.status = Status.INITIALIZING
                thread.start()
                threads.append(thread)

            for thread, node in zip(threads, nodes):
                thread.join()
                node.log(f"--------------------------- finished setup phase of {node.name} at {datetime.now()}", level="INFO")

        # set up metadata store nodes
        metadata_stores: List[BaseMetadataStoreNode] = [node for node in self.nodes if isinstance(node, BaseMetadataStoreNode) is True]
        __setup_nodes(metadata_stores)

        # set up resource nodes
        resource_nodes = [node for node in self.nodes if isinstance(node, BaseResourceNode) is True]
        __setup_nodes(resource_nodes)
        
        # set up action nodes
        action_nodes = [node for node in self.nodes if isinstance(node, BaseActionNode) is True]
        __setup_nodes(action_nodes)

        # add nodes to metadata store's node list
        for metadata_store in metadata_stores:
            for node in self.nodes:
                metadata_store.add_node(node.name, type(node).__name__)

    def launch_nodes(self):
        """
        Lanches all the registered nodes in topological order.
        """
        self.setup_nodes()

        # start nodes
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
            node.exit()
            node.join()
        print("All nodes terminated")