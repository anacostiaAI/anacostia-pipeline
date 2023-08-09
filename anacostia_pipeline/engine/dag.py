import time
from typing import List
import networkx as nx
import json
import sys
import os
from threading import Thread
from logging import Logger

sys.path.append(os.path.abspath('..'))
sys.path.append(os.path.abspath('../anacostia_pipeline'))
if __name__ == "__main__":
    from node import BaseNode, ResourceNode, ActionNode, G
    from constants import Status
else:
    from engine.node import BaseNode, ResourceNode, ActionNode, G
    from engine.constants import Status


class DAG:
    def __init__(self, logger: Logger = None) -> None:
        if nx.is_directed_acyclic_graph(G) is False:
            print(list(nx.find_cycle(G)))
            raise Exception("Graph is not a DAG")
        
        self.threads = []

        self.nodes = list(nx.topological_sort(G))
        for node in self.nodes:
            node.set_logger(logger)

    def terminate_nodes(self) -> None:
        for node in reversed(self.nodes):
            node.set_status(Status.STOPPING)

        for thread in self.threads:
            thread.join() 
    
    def pause_nodes(self) -> None:
        # pausing node need to be done in reverse order so that the parent nodes are paused before the child nodes
        # this is because the parent nodes will continue to listen for signals from the child nodes, 
        # and if the child nodes are paused first, then the parent nodes will never receive the signals,
        # and the parent nodes will never be paused
        for node in reversed(self.nodes):
            node.set_status(Status.PAUSED)

    def run_nodes(self) -> None:
        for node in self.nodes:
            node.set_status(Status.RUNNING)

    def start(self) -> None:
        self.run_nodes()
        for node in self.nodes:
            thread = Thread(target=node.run)
            thread.start()
            self.threads.append(thread)

        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                
                print("\nPausing DAG execution...")
                self.pause_nodes()
                user_input = input("\nAre you sure you want to stop the pipeline? (yes/no) Press Enter to abort. ")

                if user_input.lower() == "yes":

                    user_input = input("Enter 'hard' for a hard stop, enter 'soft' for a soft stop? Press Enter to abort. ")

                    if user_input.lower() == "hard":
                        self.terminate_nodes()
                        break

                    elif user_input == "soft":
                        # tearing down nodes and waiting for nodes to finish executing
                        print("\nExiting... tearing down all nodes in DAG")
                        
                        self.terminate_nodes()
                        for node in reversed(self.nodes):
                            node.teardown()
                        
                        print("Dag teardown complete")                            
                        break 

                    else:
                        print("Resuming the pipeline")
                        self.run_nodes()
                else:
                    print("Resuming the pipeline")
                    self.run_nodes()

    def export_graph(self, file_path: str) -> None:
        graph = nx.to_dict_of_dicts(G)
        graph = str(graph).replace("'", '"')
        graph = json.loads(graph)

        with open(file_path, 'w') as json_file:
            json.dump(graph, json_file, indent=4)


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

    dag = DAG()
    dag.export_graph("../../tests/testing_artifacts/graph.json")
    dag.start()