from node import Node
from typing import List


class DAG:
    def __init__(self) -> None:
        self.nodes = []
        self.edges = []
    
    def topological_sort(self) -> None:
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
            node._setup()

if __name__ == "__main__":
    node1 = Node("resource1")
    node2 = Node("resource2")
    node3 = Node("resource3")
    node4 = Node("resource4")
    node5 = Node("output1", listen_to=[node1, node2])
    node6 = Node("output2", listen_to=[node3, node4])
    node7 = Node("output3", listen_to=[node5])

    dag = DAG()
    dag.nodes = [node5, node6, node1, node2, node3, node7, node4]
    dag.start()
