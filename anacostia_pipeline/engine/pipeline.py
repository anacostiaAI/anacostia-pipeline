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

from node import BaseNode, ResourceNode, ActionNode
from constants import Status
# import IPython
class InvalidNodeDependencyError(Exception):
    pass

# TODO
# Global Node Registry
# knows all nodes
# knowns which nodes are part of which pipeline
# Pipeline.start updates the registry denoting which nodes are running on THIS machine
# any access to nodes that arent marked running are assumed to be on other machines

class Pipeline:
    '''
    Pipeline is a class that is in charge of:
    1. managing nodes
    2. running them the dependency graph they form (a DAG)
    3. interfacing with a set of nodes (stop, start, pause, add, remove, etc..) 
    '''

    def __init__(self, nodes: Iterable[BaseNode], logger: Logger = None) -> None:
        self.console = Console()
        self.graph = nx.DiGraph()
        
        # Add nodes into graph 
        for n in nodes:
            self.graph.add_node(n)
            for dependent_n in n.dependent_nodes:
                self.graph.add_node(dependent_n)
                self.graph.add_edge(n, dependent_n)

        # self.graph must be a dag
        if not nx.is_directed_acyclic_graph(self.graph):
            raise InvalidNodeDependencyError("Node Dependencies do not form a Directed Acyclic Graph")
        
        self.nodes = list(nx.topological_sort(self.graph))      # switched to list to preserve topological order of nodes
        for node in self.nodes:
            node.set_logger(logger)
        
    def help_cmd(self):
        pass

    def launch_nodes(self):
        '''
        Lanches all the registered nodes
        '''
        running_nodes = set()
        with self.console.status("Initializing Nodes...") as status:
            for node in self.nodes:
                # Note: since node is a subclass of Thread, calling start() will run the run() method
                node.start()
            
            while len(running_nodes) != len(self.nodes):
                for node in self.nodes:
                    if node not in running_nodes and node.status != Status.INIT:
                        self.console.log(f"Node {node.name} Started!")
                        running_nodes.add(node)

    def pipe_cmds(self):
        pass

    def node_cmds(self):
        pass

    def pause_nodes(self) -> None:
        # pausing node need to be done in reverse order so that the parent nodes are paused before the child nodes
        # this is because the parent nodes will continue to listen for signals from the child nodes, 
        # and if the child nodes are paused first, then the parent nodes will never receive the signals,
        # and the parent nodes will never be paused
        for node in reversed(self.nodes):
            node.pause()

    def terminate_nodes(self) -> None:
        for node in self.nodes:
            node.stop()
        
        for node in self.nodes:
            node.join()

    def start(self) -> None:
        self.launch_nodes()
        self.console.print("All Nodes Launched")
        # IPython.embed()
        while True:
            try:
                cmd_string = self.console.input("> ")
                cmd = [c for c in cmd_string.strip().split() if len(c.strip()) > 0]
                self.console.print(cmd)

                match cmd:
                    case "help":
                        self.help_text()
                    case "version":
                        version = pkg_resources.get_distribution("anacostia").version
                        self.console.print(f"Version {version}")
                    case "pipe":
                        self.pipe_cmds()
                    case "node":
                        self.node_cmds()

            except KeyboardInterrupt:
                self.console.print("Ctrl+C Detected")
                self.pause_nodes()
                answer = Prompt.ask("Are you sure you want to shutdown the pipeline?", console=self.console, default='n')

                if answer == 'y':

                    answer = Prompt.ask(
                        "Do you want to do a hard shutdown, soft shutdown, or abort the shutdown?", 
                        console=self.console, default='abort',
                        choices=['hard', 'soft', 'abort']
                    )
                    
                    if answer == 'hard':
                        self.console.print("Hard Shutdown")
                        self.terminate_nodes()
                        break

                    elif answer == 'soft':
                        print("Shutting down pipeline")
                        self.terminate_nodes() 
                        for node in reversed(self.nodes):
                            node.teardown()
                        print("Pipeline shutdown complete")
                        break

                    else:
                        print("Aborting shutdown, resuming pipeline")
                        for node in self.nodes:
                            node.resume()
                
                else:
                    print("Aborting shutdown, resuming pipeline")
                    for node in self.nodes:
                        node.resume()
                
    def export_graph(self, file_path: str) -> None:
        if file_path.endswith(".json"):
            graph = nx.to_dict_of_dicts(self.graph)
            graph = str(graph).replace("'", '"')
            graph = json.loads(graph)

            with open(file_path, 'w') as json_file:
                json.dump(graph, json_file, indent=4)
        else:
            raise ValueError("file_path must end with .json")

class FeatureStoreWatchNode(ActionNode):
    def __init__(self, name: str) -> None:
        super().__init__(name, "feature_store")

    def execute(self) -> None:
        print("checking feature store")
        time.sleep(2)
        print("FeatureStoreWatchNode triggered")
        return True
    
    def teardown(self) -> None:
        print("tearing down feature store")
        time.sleep(1)
        print("teardown complete")
    

class ModelRegistryWatchNode(ActionNode):
    def __init__(self, name: str) -> None:
        super().__init__(name, "model_registry")

    def execute(self) -> None:
        print("checking model registry")
        time.sleep(1)
        print("ModelRegistryWatchNode triggered")
        return True

    def teardown(self) -> None:
        print("tearing down model registry")
        time.sleep(1)
        print("teardown complete")


class TrainNode(ActionNode):
    def __init__(self, name: str, listen_to: List[BaseNode] = ...) -> None:
        super().__init__(name, "train", listen_to)
    
    def execute(self) -> None:
        print("train_model triggered")
        time.sleep(3)
        print("train_model finished")
    
    def teardown(self) -> None:
        print("tearing down training node")
        time.sleep(1)
        print("teardown complete")


if __name__ == "__main__":
    feature_store_node = FeatureStoreWatchNode("feature store")
    model_registry_node = ModelRegistryWatchNode("model registry")
    train_node = TrainNode("train_model", listen_to=[feature_store_node, model_registry_node])

    dag = Pipeline([feature_store_node, model_registry_node, train_node])
    dag.export_graph("../../tests/testing_artifacts/graph.json")
    dag.start()