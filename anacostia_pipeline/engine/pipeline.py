from typing import List, Iterable
from threading import Thread
from datetime import datetime
import time
import sys
import os
from logging import Logger
import networkx as nx

sys.path.append(os.path.abspath('..'))
sys.path.append(os.path.abspath('../anacostia_pipeline'))
if __name__ == "__main__":
    from base import BaseActionNode, BaseResourceNode, BaseNode, BaseMetadataStoreNode
    from constants import Status
else:
    from engine.base import BaseActionNode, BaseResourceNode, BaseNode, BaseMetadataStoreNode
    from engine.constants import Status



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

    def __init__(self, nodes: Iterable[BaseNode], anacostia_path: str = None, logger: Logger = None) -> None:
        self.graph = nx.DiGraph()

        if anacostia_path is not None:
            self.anacostia_path = os.path.join(anacostia_path, "anacostia")
        else:
            self.anacostia_path = os.path.join(os.path.abspath('.'), "anacostia")
        
        os.makedirs(self.anacostia_path, exist_ok=True)
        
        # Set anacostia path for all nodes
        for node in nodes:
            node.set_anacostia_path(self.anacostia_path)
        
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

        # check to make sure graph is acyclic (i.e., check if graph is a DAG)
        if not nx.is_directed_acyclic_graph(self.graph):
            raise InvalidNodeDependencyError("Node Dependencies do not form a Directed Acyclic Graph")

        self.nodes: List[BaseNode] = list(nx.topological_sort(self.graph))

        # check to make sure root node is a metadata store node
        if isinstance(self.nodes[0], BaseMetadataStoreNode) is not True:
            raise InvalidNodeDependencyError("Root node must be a metadata store node")
        
        # TODO: check to make sure there is only one metadata store node
        for node in self.nodes:
            if (node != self.nodes[0]) and (isinstance(nodes, BaseMetadataStoreNode) is True):
                raise InvalidNodeDependencyError("There can only be one metadata store node")
                
        self.metadata_store = self.nodes[0]

        # TODO: check to make sure all resource nodes are successors of the metadata store node
        # TODO: check to make sure graph is not disconnected
        # TODO: check to make sure all resource nodes are predecessors of an action node 

    def launch_nodes(self):
        """
        Lanches all the registered nodes in topological order.
        """

        # set up nodes
        threads = []
        for node in self.nodes:
            node.log(f"--------------------------- started setup phase of {node.name} at {datetime.now()}")
            thread = Thread(target=node.setup)
            node.status = Status.INIT
            thread.start()
            threads.append(thread)
        
        for thread, node in zip(threads, self.nodes):
            thread.join()
            node.status = Status.RUNNING
            node.log(f"--------------------------- finished setup phase of {node.name} at {datetime.now()}")

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
