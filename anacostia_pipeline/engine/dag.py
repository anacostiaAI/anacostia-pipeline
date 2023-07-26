from node import ActionNodes, G
from typing import List
import time
import networkx as nx
import json


class DAG:
    def __init__(self) -> None:
        self.nodes = []
        self.edges = []
    
    def start(self) -> None:
        for node in nx.topological_sort(G):
            node.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nExiting...")
            sorted_nodes = list(nx.topological_sort(G))
            for node in sorted_nodes:
                node.teardown()
            print("All nodes teardown complete")
            exit(0)

    def export_graph(self, file_path: str) -> None:
        graph = nx.to_dict_of_dicts(G)
        graph = str(graph).replace("'", '"')
        graph = json.loads(graph)

        with open(file_path, 'w') as json_file:
            json.dump(graph, json_file, indent=4)

if __name__ == "__main__":
    def resource1():
        print("checking resource1")
        time.sleep(1)
        print("resource1 triggered")
        return True
    
    def resource2():
        print("checking resource2")
        time.sleep(2)
        print("resource2 triggered")
        return True
    
    def train_model():
        print("train_model triggered")
        time.sleep(3)
        print("train_model finished")
        return True

    node1 = ActionNodes("resource1", "resource", resource1)
    node2 = ActionNodes("resource2", "resource", resource2)
    node3 = ActionNodes("train_model", "resource", train_model, listen_to=[node1, node2])

    dag = DAG()
    #dag.start()
    dag.export_graph("/Users/minhquando/Desktop/anacostia/anacostia_pipeline/resource/folder1/graph.json")