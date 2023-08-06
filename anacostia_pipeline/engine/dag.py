import time
from typing import List
import networkx as nx
import json
import sys
import os
from signal import SIGTERM, SIGINT, SIGKILL, signal
from multiprocessing import Process

sys.path.append(os.path.abspath('..'))
sys.path.append(os.path.abspath('../anacostia_pipeline'))
if __name__ == "__main__":
    from node import BaseNode, ResourceNode, ActionNode, G
else:
    from engine.node import BaseNode, ResourceNode, ActionNode, G


class DAG:
    def __init__(self) -> None:
        if nx.is_directed_acyclic_graph(G) is False:
            print(list(nx.find_cycle(G)))
            raise Exception("Graph is not a DAG")
        
        self.node_pids = []
        self.nodes = list(nx.topological_sort(G))
    
    def pause_nodes(self, signum, frame):
        for node in self.nodes:
            node.pause_execution(None, None)
    
    def terminate_nodes(self, signum, frame):
        for node in self.nodes:
            node.terminate_execution(None, None)
    
    def unpause_nodes(self, signum, frame):
        for node in self.nodes:
            node.continue_execution(None, None)
    
    def start(self) -> None:
        """
        for node in self.nodes:
            process = Process(target=node.run)
            process.start()
            self.node_pids.append(process.pid)

        signal(SIGINT, self.pause_nodes) 
        signal(SIGTERM, self.terminate_nodes)
        """
        
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                
                user_input = input("\nAre you sure you want to stop the pipeline? (yes/no) ")
                if user_input == "yes":

                    user_input = input("Do you want to hard stop or soft stop or abort? (hard/soft/abort) ")
                    if user_input == "hard":
                        exit(0)

                    elif user_input == "soft":
                        print("\nExiting... tearing down all nodes in DAG")

                        time.sleep(2)
                        """
                        for node, pid in zip(self.nodes, self.node_pids):
                            os.kill(pid, SIGTERM)
                            node.process.join()
                            node.teardown()
                        """

                        print("All nodes teardown complete")
                        exit(0)

                    elif user_input == "abort":
                        continue
                    else:
                        continue
                else:
                    continue


    def export_graph(self, file_path: str) -> None:
        graph = nx.to_dict_of_dicts(G)
        graph = str(graph).replace("'", '"')
        graph = json.loads(graph)

        with open(file_path, 'w') as json_file:
            json.dump(graph, json_file, indent=4)


class FeatureStoreWatchNode(ResourceNode):
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
    

class ModelRegistryWatchNode(ResourceNode):
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
    #model_registry_node = ModelRegistryWatchNode("model registry")
    #train_node = TrainNode("train_model", listen_to=[feature_store_node, model_registry_node])

    dag = DAG()
    dag.start()
    #dag.export_graph("/Users/minhquando/Desktop/anacostia/anacostia_pipeline/resource/folder1/graph.json")