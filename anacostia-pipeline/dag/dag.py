from node import Node, G
from typing import List
import time
import networkx as nx


class DAG:
    def __init__(self) -> None:
        self.nodes = []
        self.edges = []
    
    def start(self) -> None:
        for node in nx.topological_sort(G):
            node.setup()
        
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

    node1 = Node("resource1", resource1)
    node2 = Node("resource2", resource2)
    node3 = Node("train_model", train_model, listen_to=[node1, node2])

    dag = DAG()
    dag.start()