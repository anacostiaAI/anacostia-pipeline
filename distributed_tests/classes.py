from fastapi import FastAPI
import uvicorn
from typing import List



class NodeApp(FastAPI):
    def __init__(self, name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.graph_prefix = None    # graph_prefix variable is set by GraphApp when NodeApp is mounted

        @self.get("/info")
        def node_info():
            # http://localhost:8000/<graph name>/<node name>/info
            return f"{self.get_full_prefix()} info"
    
    def get_node_prefix(self):
        return f"/{self.name}"
    
    def get_full_prefix(self):
        return f"{self.graph_prefix}{self.get_node_prefix()}"



class GraphApp(FastAPI):
    def __init__(self, name: str, nodes: List[NodeApp], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

        for node in nodes:
            node.graph_prefix = self.get_graph_prefix()
            self.mount(node.get_node_prefix(), node)

        @self.get("/info")
        def graph_info():
            # http://localhost:8000/<graph name>/info
            return f"{self.name} info"

    def get_graph_prefix(self):
        return f"/{self.name}"



class ServiceApp(FastAPI):
    def __init__(self, name: str, graphs: List[GraphApp], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

        for graph in graphs:
            self.mount(graph.get_graph_prefix(), graph)

        @self.get("/")
        def service_info():
            # http://localhost:8000/
            return f"{self.name} info"



if __name__ == "__main__":
    node1 = NodeApp("node1")
    node2 = NodeApp("node2")
    node3 = NodeApp("node3")
    node4 = NodeApp("node4")

    graph1 = GraphApp("graph1", [node1, node2])
    graph2 = GraphApp("graph2", [node3, node4])

    service = ServiceApp("service", [graph1, graph2])

    config = uvicorn.Config(service, host="localhost", port=8000)
    server = uvicorn.Server(config)
    server.run()