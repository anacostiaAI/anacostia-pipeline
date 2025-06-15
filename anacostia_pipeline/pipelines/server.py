import threading
import signal
from logging import Logger
from contextlib import asynccontextmanager
from queue import Queue
import asyncio
from pydantic import BaseModel
from typing import List, Dict, Any
import sys
import os
import json
from urllib.parse import urlparse

import uvicorn
import httpx
from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from anacostia_pipeline.nodes.utils import NodeConnectionModel, NodeModel
from anacostia_pipeline.pipelines.pipeline import Pipeline, InvalidPipelineError, InvalidNodeDependencyError
from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.connector import Connector
from anacostia_pipeline.nodes.api import BaseClient
from anacostia_pipeline.nodes.api import BaseServer
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.pipelines.fragments import node_bar_closed, node_bar_open, node_bar_invisible, index_template


class PipelineConnectionModel(BaseModel):
    predecessor_host: str
    predecessor_port: int

class EventModel(BaseModel):
    event: str
    data: str


class PipelineServer(FastAPI):
    def __init__(
        self, 
        name: str, 
        pipeline: Pipeline, 
        remote_clients: List[BaseClient] = None,
        host: str = "127.0.0.1", 
        port: int = 8000, 
        ssl_keyfile: str = None, 
        ssl_certfile: str =None, 
        logger: Logger = None, 
        uvicorn_access_log_config: Dict[str, Any] = None,
        allow_origins: List[str] = ["*"],
        allow_credentials: bool = False,
        allow_methods: List[str] = ["*"],
        allow_headers: List[str] = ["*"],
        *args, **kwargs
    ):
        if remote_clients is not None:
            num_metadata_clients = len([client for client in remote_clients if isinstance(client, BaseMetadataStoreClient)])
            if num_metadata_clients > 1:
                raise InvalidPipelineError("Only one BaseMetadataStoreClient is allowed in the pipeline. Found multiple metadata store nodes.")

        # lifespan context manager for spinning up and shutting down the service
        @asynccontextmanager
        async def lifespan(app: PipelineServer):
            if app.logger is not None:
                app.logger.info(f"Pipeline server '{app.name}' started")

            # queue must be set for all nodes prior to starting the background task
            for node in app.pipeline.nodes:
                node.set_queue(app.queue)

            app.background_task = asyncio.create_task(app.process_queue())

            await app.connect()     # Connect to the leaf services

            yield

            # Cancel and cleanup the background task when the app shuts down
            app.background_task.cancel()
            try:
                await app.background_task
            except asyncio.CancelledError:
                pass

            if app.logger is not None:
                app.logger.info(f"Pipeline server '{app.name}' shut down")
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.pipeline = pipeline
        self.host = host
        self.port = port
        self.logger = logger
        self.successor_pipeline_models = []
        self.queue = Queue()
        self.background_task = None

        if allow_credentials is True and allow_origins == ["*"]:
            raise ValueError("allow_origins cannot be [\"*\"] when allow_credentials = True")
        
        self.add_middleware(
            CORSMiddleware,
            allow_origins=allow_origins,
            allow_credentials=allow_credentials,
            allow_methods=allow_methods,
            allow_headers=allow_headers,
        )

        # Mount the static files directory to the webserver
        DASHBOARD_DIR = os.path.dirname(sys.modules["anacostia_pipeline"].__file__)
        self.static_dir = os.path.join(DASHBOARD_DIR, "static")
        self.mount("/static", StaticFiles(directory=self.static_dir), name="webserver")

        config = uvicorn.Config(self, host=self.host, port=self.port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, log_config=uvicorn_access_log_config)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)

        # get the successor ip addresses
        self.successor_ip_addresses = []
        for node in self.pipeline.nodes:
            for url in node.remote_successors:
                parsed = urlparse(url)
                base_url = f"{parsed.scheme}://{parsed.netloc}"

                if base_url not in self.successor_ip_addresses:
                    self.successor_ip_addresses.append(base_url)

        # get metadata store from the pipeline
        self.metadata_store = None
        for node in self.pipeline.nodes:
            if isinstance(node, BaseMetadataStoreNode):
                self.metadata_store = node
                break

        # Mount the apps and connectors to the webserver
        for node in self.pipeline.nodes:
            connector: Connector = node.setup_connector(host=self.host, port=self.port)
            self.mount(connector.get_connector_prefix(), connector)          

            node_gui: BaseGUI = node.setup_node_GUI(host=self.host, port=self.port)
            self.mount(node_gui.get_node_prefix(), node_gui)                # mount the BaseNodeApp to PipelineWebserver
        
            server: BaseServer = node.setup_node_server(host=self.host, port=self.port)
            self.mount(server.get_node_prefix(), server)                    # mount the BaseRPCserver to PipelineWebserver

        if remote_clients is not None:
            for rpc_client in remote_clients:
                rpc_client.add_loggers(self.logger)                             # add the logger to the rpc_client
                rpc_client.client_host = self.host
                rpc_client.client_port = self.port
                self.mount(rpc_client.get_client_prefix(), rpc_client)          # mount the BaseRPCclient to PipelineWebserver
        
        self.predecessor_host = None
        self.predecessor_port = None
        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(connection: PipelineConnectionModel):
            self.predecessor_host = connection.predecessor_host
            self.predecessor_port = connection.predecessor_port
            self.logger.info(f"Leaf server {self.name} connected to root server at {self.predecessor_host}:{self.predecessor_port}")
            return self.frontend_json()
        
        self.connected = False
        @self.post("/finish_connect", status_code=status.HTTP_200_OK)
        async def finish_connect():
            for node in self.pipeline.nodes:
                node.connection_event.set()  # Set the connection event for each node
            self.connected = True
        
        # in cases where there are other leaf pipelines connected to this leaf pipeline (e.g., root -> leaf1 -> leaf2), 
        # the /send_event endpoint enables leaf2 to relay its messages to leaf1 by putting its messages into leaf1's queue,
        # leaf1 then relays all of its messages and leaf2's messages back to the root pipeline
        @self.post('/send_event')
        async def send_event(message: EventModel):
            self.queue.put_nowait(message.model_dump())
            return {"status": "ok"}
    
        @self.get('/', response_class=HTMLResponse)
        async def index(request: Request):
            frontend_json = self.frontend_json()
            nodes = frontend_json["nodes"]
            return index_template(nodes, frontend_json, "/graph_sse")

        @self.get("/header_bar", response_class=HTMLResponse)
        async def header_bar(node_id: str, visibility: bool = False):
            html_responses = []
            frontend_json = self.frontend_json()

            node_models = frontend_json["nodes"]
            for node_model in node_models:
                if node_model["id"] != node_id:
                    snippet = node_bar_invisible(node_model=node_model)
                else:
                    if visibility is False:
                        snippet = node_bar_closed(node_model=node_model, open_div_endpoint=f'/header_bar/?node_id={node_model["id"]}&visibility=true')
                    else:
                        snippet = node_bar_open(node_model=node_model, close_div_endpoint=f'/header_bar/?node_id={node_model["id"]}&visibility=false') 

                html_responses.append(snippet)

            return "\n".join(html_responses)
        
        self.recent_messages = {}
        @self.get('/graph_sse', response_class=StreamingResponse)
        async def graph_sse(request: Request):
            async def event_stream():

                # when the home page loads, if the queue is empty, display the most recent status of each node
                for node_id, node_status in self.recent_messages.items():
                    if node_status != "INITIALIZING":
                        message_data = json.dumps({"id": node_id, "status": node_status})
                        yield f"event: WorkUpdate\n"
                        yield f"data: {message_data}\n\n"

                # if the queue is not empty, display the status of the nodes in the queue
                while True:
                    try:
                        if self.queue.empty() is False:
                            message = self.queue.get_nowait()
                            message_data = json.loads(message["data"])
                            self.recent_messages[message_data["id"]] = message_data["status"]

                            yield f"event: {message['event']}\n"
                            yield f"data: {message['data']}\n\n"

                        await asyncio.sleep(0.1)

                    except asyncio.CancelledError:
                        print("event source /graph_sse closed")
                        yield "event: close\n"
                        yield "data: \n\n"
                        break

            return StreamingResponse(event_stream(), media_type="text/event-stream")

        @self.get('/dag_page', response_class=HTMLResponse)
        def dag_page(response: Response):
            response.headers["HX-Redirect"] = "/"

    async def process_queue(self):
        async with httpx.AsyncClient() as client:
            while True:
                if self.queue.empty() is False and self.connected is True:
                    message = self.queue.get()
                    
                    try:
                        if self.predecessor_host is not None and self.predecessor_port is not None:
                            await client.post(f"http://{self.predecessor_host}:{self.predecessor_port}/send_event", json=message)
                    
                    except httpx.ConnectError as e:
                        self.logger.error(f"Could not connect to root server at {self.predecessor_host}:{self.predecessor_port} - {str(e)}")
                        self.queue.put(message)
                        self.connected = False

                    except Exception as e:
                        self.logger.error(f"Error forwarding message: {str(e)}")
                        self.queue.put(message)
                        self.connected = False
                    
                    finally:
                        self.queue.task_done()
                
                try:
                    await asyncio.sleep(0.1)
                except asyncio.CancelledError:
                    self.logger.info("Background task cancelled; breaking out of queue processing loop")
                    break 

    async def connect(self):
        async with httpx.AsyncClient() as client:
            # Connect to leaf pipeline
            task = []
            for leaf_ip_address in self.successor_ip_addresses:
                pipeline_server_model = PipelineConnectionModel(predecessor_host=self.host, predecessor_port=self.port).model_dump()
                task.append(client.post(f"{leaf_ip_address}/connect", json=pipeline_server_model))

            responses = await asyncio.gather(*task)

            successor_node_models: List[NodeModel] = []

            for response in responses:
                # Extract the leaf graph structure from the responses, this information will be used to construct the graph on the frontend
                response_data = response.json()
                self.successor_pipeline_models.append(response_data)

                # Validate the response data and extract node models
                for node_data in response_data["nodes"]:
                    node_data = NodeModel.model_validate(node_data)
                    successor_node_models.append(node_data)

            # Extract the node name and type from the responses and add them to the metadata store
            if self.metadata_store is not None:
                for node_model in successor_node_models:
                    node_name = node_data.name
                    node_type = node_data.node_type
                    base_type = node_data.base_type
            
                    if self.metadata_store.node_exists(node_name=node_name) is False:
                        self.metadata_store.add_node(node_name=node_name, node_type=node_type, base_type=base_type)

            # ------ Check graph structure of when pipelines before they are connected ------
            for node in self.pipeline.nodes:
                node_base_type = node.model().base_type

                for connection in node.remote_successors:
                    remote_node_name = connection.split("/")[-1]

                    for node_model in successor_node_models:
                        if node_model.name == remote_node_name:
                            remote_node_base_type = node_data.base_type
                            
                            # based on the remote_successors information, check if the connection is valid
                            if node_base_type == "BaseMetadataStoreNode" and remote_node_base_type != "BaseResourceNode":
                                raise InvalidNodeDependencyError(
                                    f"Invalid connection: Metadata store node '{node.name}' cannot connect to non-resource node '{remote_node_name}'"
                                )
                            
                            if node_base_type == "BaseResourceNode" and remote_node_base_type != "BaseActionNode":
                                raise InvalidNodeDependencyError(
                                    f"Invalid connection: Resource node '{node.name}' cannot connect to non-action node '{remote_node_name}'"
                                )
            # ------------------------------------------------------------------

            # Connect each node to its remote successors
            task = []
            for node in self.pipeline.nodes:
                for connection in node.remote_successors:
                    node_model = node.model()
                    connection_mode = NodeConnectionModel(
                        **node_model.model_dump(),
                        node_url=f"http://{self.host}:{self.port}/{node.name}"
                    )
                    json = connection_mode.model_dump()
                    task.append(client.post(f"{connection}/connector/connect", json=json))

            await asyncio.gather(*task)

            # Connect RPC servers to RPC clients
            task = []
            for node in self.pipeline.nodes:
                task.append(node.node_server.connect())

            await asyncio.gather(*task)

            # Finish the connection process for each leaf pipeline
            # Leaf pipeline will set the connection event for each node
            task = []
            for leaf_ip_address in self.successor_ip_addresses:
                task.append(client.post(f"{leaf_ip_address}/finish_connect"))

            await asyncio.gather(*task)

    def frontend_json(self):
        model = self.pipeline.pipeline_model.model_dump()
        edges = [{"source": predecessor, "target": successor} for predecessor, successor in model["edges"]]

        for node_model, node in zip(model["nodes"], self.pipeline.nodes):
            subapp = node.get_node_gui()
            node_model["id"] = node_model["name"]
            node_model["label"] = node_model["name"]
            node_model["origin_url"] = f"http://{self.host}:{self.port}"
            node_model["type"] = type(node).__name__
            
            subapp_home_endpoint = subapp.get_home_endpoint()
            node_model["endpoint"] = f"http://{self.host}:{self.port}{subapp_home_endpoint}" if subapp_home_endpoint else ''
            
            node_model["status_endpoint"] = f"http://{self.host}:{self.port}{subapp.get_status_endpoint()}"
            node_model["header_bar_endpoint"] = f'/header_bar/?node_id={node_model["id"]}'

            # add the remote successors to the node model
            for remote_successor_url in node.remote_successors:
                parsed = urlparse(remote_successor_url)
                successor_name = parsed.path.split("/")[-1]
                edges.append({"source": node_model["id"], "target": successor_name})

        # add the leaf nodes to the model
        for leaf_graph_model in self.successor_pipeline_models:
            for leaf_node_model in leaf_graph_model["nodes"]:
                leaf_node_model["header_bar_endpoint"] = f'/header_bar/?node_id={leaf_node_model["id"]}'

            model["nodes"].extend(leaf_graph_model["nodes"])
            edges.extend(leaf_graph_model["edges"])

        model["edges"] = edges
        return model

    def run(self):
        # Store original signal handlers
        original_sigint_handler = signal.getsignal(signal.SIGINT)
        original_sigterm_handler = signal.getsignal(signal.SIGTERM)

        def _kill_webserver(sig, frame):

            # Stop the server
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print(f"Anacostia leaf webserver {self.name} Killed...")

            # Terminate the pipeline
            print("Killing leaf pipeline...")
            self.pipeline.terminate_nodes()
            print("Leaf pipeline Killed.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)
            signal.signal(signal.SIGTERM, original_sigterm_handler)

            # If this was SIGTERM, we might want to exit the process
            if sig == signal.SIGTERM:
                sys.exit(0)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        signal.signal(signal.SIGTERM, _kill_webserver)

        # Start the webserver
        self.fastapi_thread.start()

        self.pipeline.launch_nodes()

        # keep the main thread open; this is done to avoid an error in python 3.12 "RuntimeError: can't create new thread at interpreter shutdown"
        # and to avoid "RuntimeError: can't register atexit after shutdown" in python 3.9
        for thread in threading.enumerate():
            if thread.daemon or thread is threading.current_thread():
                continue
            thread.join()