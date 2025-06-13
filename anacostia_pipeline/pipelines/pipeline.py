from typing import List, Iterable, Union, Tuple
from threading import Thread
from datetime import datetime
from logging import Logger
import signal

from pydantic import BaseModel, ConfigDict
import networkx as nx

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.utils import NodeModel
from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.utils.constants import Status


class InvalidNodeDependencyError(Exception):
    pass

class InvalidPipelineError(Exception):
    pass


class PipelineModel(BaseModel):
    '''
    A Pydantic Model for validation and serialization of a Pipeline
    '''
    model_config = ConfigDict(from_attributes=True)
    nodes: List[NodeModel]
    edges: List[Tuple[str, str]]


class Pipeline:
    """
    Pipeline is a class that is in charge of graph management and execution; this includes:
    1. Providing an API to interact with the nodes.
        - Pipeline class will be used to create a CLI and a browser GUI.
        - CLI commands include help, version, start, shutdown, pause, resume, and check_status. More commands will be added later.
        - Browser GUI will be added later.
    2. Saving graph as graph.json file and loading a graph.json file back into the pipeline to recreate the DAG. 
    2. Ensuring the user built the graph correctly (i.e., ensuring the graph is a DAG)
    """

    def __init__(self, name: str, nodes: Iterable[BaseNode], loggers: Union[Logger, List[Logger]] = None) -> None:
        self.node_dict = dict()
        self.graph = nx.DiGraph()
        self.name = name

        # Add nodes into graph
        for node in nodes:
            self.graph.add_node(node)
            self.node_dict[node.name] = node

        # Add edges into graph
        for node in nodes:
            for predecessor in node.predecessors:
                self.graph.add_edge(predecessor, node)
        
        for node in nodes:
            for successor in node.successors:
                if successor not in nodes:
                    raise ValueError(f"Node '{successor.name}' has not been registered with pipeline. Check the 'nodes' parameter in the Pipeline class")
        
        # Set logger for all nodes
        if loggers is not None:
            for node in nodes:
                node.add_loggers(loggers)
        
        """
        test for the following conditions:
        1. The overall graph is a Directed Acyclic Graph (DAG).
        2. The overall graph is not disconnected.
        3. All the successors of metadata store nodes are resource nodes.
        4. All the successors of resource nodes are action nodes.
        Note: checks in the Pipeline class are to check the structure of the local graph.
        Note: PipelineServer class collects graph information from the Pipeline class (via its PipelineModel)
        as well as from the PipelineServer.frontend_json() method. json data will be converted to dictionary entries 
        and converted to networkx DiGraph like so
        >>> graph_data = PipelineServer.frontend_json()
        >>> self.graph = nx.DiGraph(graph_data)
        >>> self.verify_graph_structure(self.graph)
        Note: in the future, PipelineServer will render BaseClient objects as disconnected nodes in the graph.
        Note: BaseClient objects are not part of the PipelineModel, but they are part of the PipelineServer's frontend_json.
        Note: look into nx.from_dict_of_lists() to convert a dictionary of lists to a networkx DiGraph.
        Note: also look into nx.from_edge_list() to convert a list of edges to a networkx DiGraph.
        """

        # Perform checks on the local graph structure
        # check 1: make sure graph is acyclic (i.e., check if graph is a DAG)
        if not nx.is_directed_acyclic_graph(self.graph):
            raise InvalidNodeDependencyError("Node Dependencies do not form a Directed Acyclic Graph")
        
        self.nodes: List[BaseNode] = list(nx.topological_sort(self.graph))

        # check 2: make sure all successors of metadata store nodes are resource nodes.
        for node in self.nodes:
            if isinstance(node, BaseMetadataStoreNode) is True:
                for successor in list(self.graph.successors(node)):
                    if isinstance(successor, BaseResourceNode) is False:
                        raise InvalidNodeDependencyError("All successors of a metadata store node must be resource nodes")

        # check 3: make sure all successors of a resource node are action nodes 
        for node in self.nodes:
            if isinstance(node, BaseResourceNode) is True:
                for successor in list(self.graph.successors(node)):
                    if isinstance(successor, BaseActionNode) is False:
                        raise InvalidNodeDependencyError("All successors of a resource node must be action nodes")

        self.pipeline_model = PipelineModel(
            nodes=[n.model() for n in self.nodes],
            edges=[(str(predecessor.name), str(successor.name)) for predecessor, successor in self.graph.edges()]
        )

    def verify_graph_structure(self, edge_list: List[Tuple[str, str]]) -> None:
        pass

    def __getitem__(self, key):
        return self.node_dict.get(key, None)
    
    def setup_nodes(self):
        """
        Sets up all the registered nodes in topological order.
        """

        def __setup_nodes(nodes: List[BaseNode]):
            """
            Sets up all the nodes in the pipeline.
            """

            threads: List[Thread] = []
            for node in nodes:
                node.log(f"--------------------------- started setup phase of {node.name} at {datetime.now()}")
                thread = Thread(target=node.setup)
                node.status = Status.INITIALIZING
                thread.start()
                threads.append(thread)

            for thread, node in zip(threads, nodes):
                thread.join()
                node.log(f"--------------------------- finished setup phase of {node.name} at {datetime.now()}")

        # set up metadata store nodes
        metadata_stores: List[BaseMetadataStoreNode] = [node for node in self.nodes if isinstance(node, BaseMetadataStoreNode) is True]
        if len(metadata_stores) > 1:
            raise InvalidPipelineError("Only one metadata store node is allowed in the pipeline.")
        __setup_nodes(metadata_stores)

        # set up resource nodes
        resource_nodes = [node for node in self.nodes if isinstance(node, BaseResourceNode) is True]
        __setup_nodes(resource_nodes)
        
        # set up action nodes
        action_nodes = [node for node in self.nodes if isinstance(node, BaseActionNode) is True]
        __setup_nodes(action_nodes)

        # add nodes to metadata store's node list
        if len(metadata_stores) > 0:
            for metadata_store in metadata_stores:
                for node in self.nodes:
                    node_model: NodeModel = node.model()
                    metadata_store.add_node(node_model.name, node_model.node_type, node_model.base_type)

    # consider renaming this method to start_pipeline
    def launch_nodes(self):
        """
        Lanches all the registered nodes in topological order.
        """
        self.setup_nodes() 

        # Note: since node is a subclass of Thread, calling start() will run the run() method, thereby starting the node
        for node in self.nodes:
            node.start()

    # consider renaming this method to shutdown_pipeline
    def terminate_nodes(self) -> None:
        # terminating nodes need to be done in reverse order so that the successor nodes are terminated before the predecessor nodes
        # this is because the successor nodes will continue to listen for signals from the predecessor nodes,
        # and if the predecessor nodes are terminated first, then the sucessor nodes will never receive the signals,
        # thus, the successor nodes will never be terminated.
        # predecessor nodes need to wait for the successor nodes to terminate before they can terminate. 

        print("Terminating nodes")
        for node in reversed(self.nodes):
            node.exit()
            node.join()
        print("All nodes terminated")

    # Note: this run method is only here to run the pipeline without the webserver
    # This method might be deprecated.
    def run(self) -> None:
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print("\nCTRL+C Caught!; Shutting down nodes...")
            self.terminate_nodes()
            print("Nodes shutdown.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)

        print("Launching Pipeline...")
        self.launch_nodes()