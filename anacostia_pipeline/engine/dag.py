import time
from typing import List
import networkx as nx
import json
import sys
import os
from multiprocessing import Process, Value
from logging import Logger

sys.path.append(os.path.abspath('..'))
sys.path.append(os.path.abspath('../anacostia_pipeline'))
if __name__ == "__main__":
    from node import BaseNode, ResourceNode, ActionNode, G
else:
    from engine.node import BaseNode, ResourceNode, ActionNode, G

from constants import Status


class DAG:
    def __init__(self, logger: Logger = None) -> None:
        if nx.is_directed_acyclic_graph(G) is False:
            print(list(nx.find_cycle(G)))
            raise Exception("Graph is not a DAG")
        
        self.node_pids = []
        self.processes = []

        self.nodes = list(nx.topological_sort(G))
        for node in self.nodes:
            node.set_logger(logger)

    def __new__(cls, *args, **kwargs):
        # Singleton pattern to ensure only one DAG exists
        if cls.__instance is None:
            cls.__instance = super(DAG, cls).__new__(cls)
            return cls.__instance
        else:
            raise Exception("DAG already exists")

    def start(self) -> None:
        # Create a multiprocessing value to control the loop execution
        resume_flag = Value('i', int(Status.RUNNING)) 

        for node in self.nodes:
            process = Process(target=node.run, args=(resume_flag,))
            process.start()
            self.node_pids.append(process.pid)
            self.processes.append(process)

        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                
                print("\nPausing DAG execution...")
                resume_flag.value = int(Status.PAUSED)
                user_input = input("\nAre you sure you want to stop the pipeline? (yes/no) Press Enter to abort. ")

                if user_input.lower() == "yes":

                    user_input = input("Enter 'hard' for a hard stop, enter 'soft' for a soft stop? Press Enter to abort. ")

                    if user_input.lower() == "hard":
                        resume_flag.value = int(Status.STOPPING)
                        for process in self.processes:
                            process.join()
                        break

                    elif user_input == "soft":
                        resume_flag.value = int(Status.STOPPING)

                        # tearing down nodes and waiting for nodes to finish executing
                        print("\nExiting... tearing down all nodes in DAG")
                        for process in self.processes:
                            process.join()

                        for node in reversed(self.nodes):
                            node.teardown()
                        print("Dag teardown complete")
                            
                        break 

                    else:
                        print("Resuming the pipeline")
                        resume_flag.value = int(Status.RUNNING)
                else:
                    print("Resuming the pipeline")
                    resume_flag.value = int(Status.RUNNING)

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