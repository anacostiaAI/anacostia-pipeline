import threading
import signal
import asyncio
from logging import Logger
from contextlib import asynccontextmanager
import os
import sys
from queue import Queue
from urllib.parse import urlparse
import json
from typing import Dict, Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.routing import Mount

import uvicorn
import httpx

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.rpc import BaseRPCCallee
from anacostia_pipeline.nodes.connector import Connector
from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.utils import EventModel
from anacostia_pipeline.pipelines.root.fragments import node_bar_closed, node_bar_open, node_bar_invisible, index_template



class RootPipelineServer(FastAPI):
    def __init__(
        self, 
        name: str, 
        pipeline: RootPipeline, 
        host: str = "127.0.0.1", 
        port: int = 8000, 
        ssl_keyfile: str = None, 
        ssl_certfile: str =None, 
        logger: Logger = None, 
        uvicorn_access_log_config: Dict[str, Any] = None,
        *args, **kwargs
    ):
        # lifespan context manager for spinning up and shutting down the service
        @asynccontextmanager
        async def lifespan(app: RootPipelineServer):
            if app.logger is not None:
                app.logger.info(f"Root server '{app.name}' started")
            
            await app.connect()     # Connect to the leaf services

            yield
            
            for route in app.routes:
                if isinstance(route, Mount) and isinstance(route.app, BaseGUI):
                    subapp: BaseGUI = route.app
                    # this currently does nothing, add code here for cleanup if needed

            if app.logger is not None:
                app.logger.info(f"Root server '{app.name}' shut down")
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.pipeline = pipeline
        self.host = host
        self.port = port
        self.logger = logger
        self.connections = []
        self.leaf_ip_addresses = []
        self.leaf_models = []
        self.leaf_node_names = []
        self.queue = Queue()

        # get metadata store from the pipeline
        for node in self.pipeline.nodes:
            if isinstance(node, BaseMetadataStoreNode):
                self.metadata_store = node
                break

        # Mount the static files directory to the webserver
        DASHBOARD_DIR = os.path.dirname(sys.modules["anacostia_pipeline"].__file__)
        self.static_dir = os.path.join(DASHBOARD_DIR, "static")
        self.mount("/static", StaticFiles(directory=self.static_dir), name="webserver")

        config = uvicorn.Config(self, host=self.host, port=self.port, ssl_certfile=ssl_certfile, ssl_keyfile=ssl_keyfile, log_config=uvicorn_access_log_config)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)

        # get the leaf ip addresses
        for node in self.pipeline.nodes:
            for url in node.remote_successors:
                parsed = urlparse(url)
                base_url = f"{parsed.scheme}://{parsed.netloc}"

                if base_url not in self.leaf_ip_addresses:
                    self.leaf_ip_addresses.append(base_url)
    
        # Mount the apps and connectors to the webserver
        for node in self.pipeline.nodes:
            connector: Connector = node.setup_connector(host=self.host, port=self.port)
            self.mount(connector.get_connector_prefix(), connector)          

            node_gui: BaseGUI = node.setup_node_GUI(host=self.host, port=self.port)
            self.mount(node_gui.get_node_prefix(), node_gui)          # mount the BaseNodeApp to PipelineWebserver
            node.set_queue(self.queue)                                      # set the queue for the node

            callee: BaseRPCCallee = node.setup_rpc_callee(host=self.host, port=self.port)
            self.mount(callee.get_node_prefix(), callee)                    # mount the BaseRPCCallee to PipelineWebserver

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
        
        @self.post('/send_event')
        async def send_event(message: EventModel):
            self.queue.put_nowait(message.model_dump())
            return {"status": "ok"}
        
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

    async def connect(self):
        async with httpx.AsyncClient() as client:
            # Connect to leaf pipeline
            task = []
            for leaf_ip_address in self.leaf_ip_addresses:
                root_server_model={
                    "root_host": self.host, 
                    "root_port": self.port 
                }
                task.append(client.post(f"{leaf_ip_address}/connect", json=root_server_model))

            responses = await asyncio.gather(*task)

            for response in responses:
                # Extract the leaf graph structure from the responses, this information will be used to construct the graph on the frontend
                response_data = response.json()
                self.leaf_models.append(response_data)

                # Extract the node name and type from the responses and add them to the metadata store
                for node_data in response_data["nodes"]:
                    node_name = node_data["name"]
                    node_type = node_data["type"]
            
                    if self.metadata_store.node_exists(node_name=node_name) is False:
                        self.metadata_store.add_node(node_name=node_name, node_type=node_type)

            # Connect each node to its remote successors
            task = []
            for node in self.pipeline.nodes:
                for connection in node.remote_successors:
                    connection_mode = {
                        "node_url": f"http://{self.host}:{self.port}/{node.name}",
                        "node_name": node.name,
                        "node_type": type(node).__name__
                    }
                    task.append(client.post(f"{connection}/connector/connect", json=connection_mode))

            responses = await asyncio.gather(*task)

            # Connect RPC callees to RPC callers
            task = []
            for node in self.pipeline.nodes:
                task.append(node.rpc_callee.connect())
            
            responses = await asyncio.gather(*task)

            # Finish the connection process for each leaf pipeline
            # Leaf pipeline will set the connection event for each node
            task = []
            for leaf_ip_address in self.leaf_ip_addresses:
                task.append(client.post(f"{leaf_ip_address}/finish_connect"))

            responses = await asyncio.gather(*task)
        
    def frontend_json(self):
        model = self.pipeline.pipeline_model.model_dump()
        edges = []
        for node_model, node in zip(model["nodes"], self.pipeline.nodes):
            subapp = node.get_node_gui()
            node_model["id"] = node_model["name"]
            node_model["label"] = node_model["name"]
            node_model["origin_url"] = f"http://{self.host}:{self.port}"
            node_model["type"] = type(node).__name__
            node_model["endpoint"] = f"http://{self.host}:{self.port}{subapp.get_endpoint()}"
            node_model["status_endpoint"] = f"http://{self.host}:{self.port}{subapp.get_status_endpoint()}"
            node_model["header_bar_endpoint"] = f'/header_bar/?node_id={node_model["id"]}'

            # add the remote successors to the node model
            for remote_successor_url in node.remote_successors:
                parsed = urlparse(remote_successor_url)
                successor_name = parsed.path.split("/")[-1]
                node_model["successors"].append(successor_name)

            # add the edges from the node to its successors
            edges_from_node = [
                {"source": node_model["id"], "target": successor} for successor in node_model["successors"]
            ]
            edges.extend(edges_from_node)

        # add the leaf nodes to the model
        for leaf_graph_model in self.leaf_models:
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
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            print("Killing root pipeline...")
            self.pipeline.terminate_nodes()
            print("Root pipeline Killed.")

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

        # Launch the root pipeline
        self.pipeline.launch_nodes()

        # keep the main thread open; this is done to avoid an error in python 3.12 "RuntimeError: can't create new thread at interpreter shutdown"
        # and to avoid "RuntimeError: can't register atexit after shutdown" in python 3.9
        for thread in threading.enumerate():
            if thread.daemon or thread is threading.current_thread():
                continue
            thread.join()