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
    from node import BaseNode, ResourceNode, ActionNode
    from constants import Status
else:
    from engine.node import BaseNode, ResourceNode, ActionNode
    from engine.constants import Status

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
        
    def motd(self):
        msg = \
r'''
    _                                     _    _          ____   _               _  _              
   / \    _ __    __ _   ___   ___   ___ | |_ (_)  __ _  |  _ \ (_) _ __    ___ | |(_) _ __    ___ 
  / _ \  | '_ \  / _` | / __| / _ \ / __|| __|| | / _` | | |_) || || '_ \  / _ \| || || '_ \  / _ \
 / ___ \ | | | || (_| || (__ | (_) |\__ \| |_ | || (_| | |  __/ | || |_) ||  __/| || || | | ||  __/
/_/   \_\|_| |_| \__,_| \___| \___/ |___/ \__||_| \__,_| |_|    |_|| .__/  \___||_||_||_| |_| \___|
                                                                   |_|                             
'''
        self.console.print(msg)

    def help_cmd(self):
        repo_url = "TODO"

        common_commands = {
            "help": "Displays this help text",
            "version": "Prints the anacostia-pipeline module version number",
        }

        management_commands = {
            "pipe": "Manage the Pipeline",
            "node": "Manage Individual Nodes",
        }

        help_text = "\nA Machine Learning DevOps Pipeline\n\n" + \
                    "Common Commands:\n" + \
                    "\n".join(f" {cmd}\t{txt}" for cmd, txt in common_commands.items()) + "\n\n" + \
                    "Management COmmands:\n" + \
                    "\n".join(f" {cmd}\t{txt}" for cmd, txt in management_commands.items()) + "\n\n" + \
                    "Run \'COMMAND --help\' for more information on a command\n" + \
                    f"For more information see {repo_url}\n"

        self.console.print(help_text)

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
        # pausing node need to be done in reverse order so that the successor nodes are paused before the predecessor nodes
        # this is because the successor nodes will continue to listen for signals from the predecessor nodes, 
        # and if the predecessor nodes are paused first, then the sucessor nodes will never receive the signals,
        # thus, the successor nodes will never be paused
        for node in reversed(self.nodes):
            node.pause()

    def terminate_nodes(self) -> None:
        # terminating nodes need to be done in reverse order so that the successor nodes are terminated before the predecessor nodes
        # this is because the successor nodes will continue to listen for signals from the predecessor nodes,
        # and if the predecessor nodes are terminated first, then the sucessor nodes will never receive the signals,
        # thus, the successor nodes will never be terminated.
        # predecessor nodes need to wait for the successor nodes to terminate before they can terminate. 
        for node in self.nodes:
            node.stop()
            node.join()

    def start(self, cli=False) -> None:
        self.motd()
        self.console.print("Starting Pipeline!")
        self.launch_nodes()
        self.console.print("All Nodes Launched")
        if cli:
            while True:
                try:
                    cmd_string = self.console.input("> ")
                    cmd = [c for c in cmd_string.strip().split() if len(c.strip()) > 0]
                    self.console.print(cmd)

                    match cmd:
                        case "help":
                            self.help_cmd()
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
                
        else:
            # Its up to the programmer to do something about the nodes
            # from here on, otherwise the thread finishes and self would
            # assumingly get garbage collected
            pass

    def export_graph(self, file_path: str) -> None:
        if file_path.endswith(".json"):
            graph = nx.to_dict_of_dicts(self.graph)
            graph = str(graph).replace("'", '"')
            graph = json.loads(graph)

            with open(file_path, 'w') as json_file:
                json.dump(graph, json_file, indent=4)
        else:
            raise ValueError("file_path must end with .json")

"""
class FeatureStoreWatchNode(ActionNode):
    def __init__(self, name: str) -> None:
        super().__init__(name, "feature_store")

    @ActionNode.wait_successors
    def update_state(self):
        print("feature store updating state")
        time.sleep(1)
        print("feature store state updated")
        self.trigger()
        self.send_signals(Status.SUCCESS)

    def teardown(self) -> None:
        print("tearing down feature store")
        time.sleep(1)
        print("teardown complete")
    

class ModelRegistryWatchNode(ActionNode):
    def __init__(self, name: str) -> None:
        super().__init__(name, "model_registry")

    @ActionNode.wait_successors
    def update_state(self):
        print("model registry updating state")
        time.sleep(1)
        print("model registry state updated")
        self.trigger()
        self.send_signals(Status.SUCCESS)

    def teardown(self) -> None:
        print("tearing down model registry")
        time.sleep(1)
        print("teardown complete")


class TrainNode(ActionNode):
    def __init__(self, name: str, model_registry: ModelRegistryWatchNode, feature_store: FeatureStoreWatchNode) -> None:
        super().__init__(name, "train", listen_to=[model_registry, feature_store])
        self.model_registry = model_registry
        self.feature_store = feature_store
    
    def execute(self) -> None:
        print("train_model triggered")
        time.sleep(3)
        print("train_model finished")
    
    def update_state(self):
        self.model_registry.event.set()
        self.feature_store.event.set()

    def teardown(self) -> None:
        print("tearing down training node")
        time.sleep(1)
        print("teardown complete")


if __name__ == "__main__":
    feature_store_node = FeatureStoreWatchNode("feature store")
    feature_store_node.num_successors = 1
    model_registry_node = ModelRegistryWatchNode("model registry")
    model_registry_node.num_successors = 1
    train_node = TrainNode("train_model", model_registry_node, feature_store_node)
    train_node.num_predecessors = 2

    dag = Pipeline([feature_store_node, model_registry_node, train_node])
    dag.export_graph("../../tests/testing_artifacts/graph.json")
    dag.start(cli=True)
"""