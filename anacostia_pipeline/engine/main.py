from threading import Thread
from typing import Any, List
import time
import random
import networkx as nx
from flask import Flask


class Node(Thread):
    """
    A node in a pipeline. A node is a thread that runs a task.
    The run method simulates a task that takes a random amount of time to complete and updates the progress of the task.
    """

    def __init__(self, name: str, predecessors: List['Node']) -> None:
        self.progress = 0
        self.successors = list()
        self.predecessors = predecessors
        super().__init__(name=name)
    
    def run(self) -> None:
        while self.progress < 100:
            progress = self.progress + random.randint(1, 20)
            if progress > 100:
                self.progress = 100
            else:
                self.progress = progress

            print(f"{self.name} progress: {self.progress}")
            time.sleep(random.randint(1, 3))
    
    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"'Node(name: {self.name})'"
    
    def return_html(self) -> str:
        """
        Returns the html fragment for the node. 
        The html fragment is used to render the name and the progress of the node in the GUI's homepage.
        This is the html fragment that belongs in the circles that represent the nodes in the graph.
        hx-trigger will be used in conjunction with hx-get to poll the server for updates to the progress.
        """

        return f"""
        <li>{self.name}</li>
        """
    
    def return_modal(self) -> str:
        """
        Returns the html fragment for the modal for the node.
        The modal is used to render the name, the progress, and the predecessors of the node in the GUI.
        This is the html fragment that belongs in the modals that appear when the user clicks on the circles that represent the nodes in the graph.
        """
        pass


class Pipeline:
    """
    A pipeline is a collection of nodes. It is represented as a directed acyclic graph (DAG).
    This is a simplified version of the pipeline class in anacostia_pipeline/engine/cli.py.
    This class is used to demonstrate how to attach a pipeline to a GUI.
    The way anacostia is designed, the pipeline a way to coordinate the execution of nodes;
    user interfaces (e.g., CLI, GUI) will use the pipeline class to get the information it needs to create the interface.
    """
    
    def __init__(self, nodes: List[Node]) -> None:
        self.nodes = nodes
        self.graph = nx.DiGraph()

        # Add nodes into graph
        for node in self.nodes:
            self.graph.add_node(node)
        
        # Add edges into graph
        for node in self.nodes:
            for predecessor in node.predecessors:
                if self.graph.has_edge(predecessor, node) is False:
                    self.graph.add_edge(predecessor, node)
    
        # set successors for all nodes
        for node in nodes:
            node.successors = list(self.graph.successors(node))

    def run(self) -> None:
        for node in self.nodes:
            node.start()
    
    def stop(self) -> None:
        print("\npipeline stopping")
        for node in self.nodes:
            node.join()
        print("\npipeline stopped")


class GUI:
    """
    A backend for a browser-based GUI for a pipeline. 
    """

    app = Flask(__name__)

    def __init__(self, pipeline: Pipeline, host: str, port: str) -> None:
        self.pipeline = pipeline
        self.graph = pipeline.graph
        self.nodes = pipeline.nodes
        self.edges = list(self.graph.edges)
        self.host = host
        self.port = port
        self.name = "Test Pipeline"

        # since we are defining the flask app inside a class, we cannot use the regular flask decorator; 
        # thus, we must register the routes using add_url_rule
        self.app.add_url_rule("/", view_func=self.render_graph, methods=["GET"])

    def render_graph(self) -> Any:
        """
        Renders the graph as html. This is essentially the homepage of the GUI.
        This method is called when the user navigates to http://localhost:8000 in the browser and registered to the "/" route.
        From here, the user can navigate to the pages (or modals) for each node via htmx attributes.
        I recommend using plotly to render the graph as html.
        """

        # replace this function with a function that returns the html for the graph
        return f"""
        <h1>Test Pipeline</h1>
        <ul>
            {"".join([node.return_html() for node in self.nodes])}  
        </ul>
        """
    
    def run(self) -> None:
        self.app.run(host=self.host, port=self.port)
    

if __name__ == "__main__":
    
    # step 1: declare nodes and their predecessors
    node1 = Node("node1", predecessors=[])
    node2 = Node("node2", predecessors=[])
    node3 = Node("node3", predecessors=[node1, node2])
    node4 = Node("node4", predecessors=[node3])

    # step 2: declare pipeline, register nodes, and run pipeline
    pipeline = Pipeline([node1, node2, node3, node4])
    pipeline.run()

    # step 3: attach pipeline to GUI, run GUI, navigate to http://localhost:8000 in browser
    gui = GUI(pipeline, host="127.0.0.1", port="8000")
    gui.run()

    # step 4: stop pipeline
    pipeline.stop()