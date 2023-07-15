from node import Node
from typing import List
import time


class DAG:
    def __init__(self) -> None:
        self.nodes = []
        self.edges = []
    
    def topological_sort(self) -> List[Node]:
        topo = []
        visited = set()
        
        def build_topo(nodes: List[Node]):
            for node in nodes:
                if node not in visited:
                    visited.add(node)
                    build_topo(node.children)
                    topo.append(node)
        
        build_topo(self.nodes)
        return topo
    
    def start(self) -> None:
        sorted_nodes = self.topological_sort()
        for node in sorted_nodes:
            node.setup()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            for node in sorted_nodes:
                node.teardown()
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
    dag.nodes = [node1, node2, node3]
    dag.start()