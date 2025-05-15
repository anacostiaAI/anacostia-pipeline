import threading
import signal
from logging import Logger
from contextlib import asynccontextmanager
from queue import Queue
import asyncio
from pydantic import BaseModel
from typing import List, Dict, Any
import sys
from urllib.parse import urlparse

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware

import uvicorn
import httpx

from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.connector import Connector
from anacostia_pipeline.nodes.api import BaseClient
from anacostia_pipeline.nodes.api import BaseServer



class RootServerModel(BaseModel):
    root_host: str
    root_port: int



class LeafPipelineServer(FastAPI):
    def __init__(
        self, name: str, 
        pipeline: LeafPipeline, 
        rpc_clients: List[BaseClient] = None,
        host: str = "127.0.0.1", 
        port: int = 8000, 
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        uvicorn_access_log_config: Dict[str, Any] = None,
        allow_origins: List[str] = ["*"],
        allow_credentials: bool = False,
        allow_methods: List[str] = ["*"],
        allow_headers: List[str] = ["*"],
        logger=Logger, *args, **kwargs
    ):
        @asynccontextmanager
        async def lifespan(app: LeafPipelineServer):
            app.logger.info(f"Leaf server '{app.name}' started")

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

            app.logger.info(f"Leaf server '{app.name}' shut down")
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.pipeline = pipeline
        self.host = host
        self.port = port
        self.root_host = None
        self.root_port = None
        self.logger = logger
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

        config = uvicorn.Config(self, host=self.host, port=self.port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, log_config=uvicorn_access_log_config)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)

        self.leaf_ip_addresses = []
        self.leaf_models = []

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
            self.mount(node_gui.get_node_prefix(), node_gui)                # mount the BaseNodeApp to PipelineWebserver
            node.set_queue(self.queue)                                      # set the queue for the node
        
            server: BaseServer = node.setup_node_server(host=self.host, port=self.port)
            self.mount(server.get_node_prefix(), server)                    # mount the BaseRPCserver to PipelineWebserver

        if rpc_clients is not None:
            for rpc_client in rpc_clients:
                self.mount(rpc_client.get_client_prefix(), rpc_client)          # mount the BaseRPCclient to PipelineWebserver
                rpc_client.add_loggers(self.logger)                             # add the logger to the rpc_client
                rpc_client.client_host = self.host
                rpc_client.client_port = self.port

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(connection: RootServerModel):
            self.root_host = connection.root_host
            self.root_port = connection.root_port
            self.logger.info(f"Leaf server {self.name} connected to root server at {self.root_host}:{self.root_port}")
            return self.frontend_json()
        
        @self.post("/finish_connect", status_code=status.HTTP_200_OK)
        async def finish_connect():
            for node in self.pipeline.nodes:
                node.connection_event.set()  # Set the connection event for each node

        self.queue = Queue()
    
    async def process_queue(self):
        async with httpx.AsyncClient() as client:
            while True:
                if self.queue.empty() is False:
                    message = self.queue.get()
                    
                    try:
                        if self.root_host is not None and self.root_port is not None:
                            await client.post(f"http://{self.root_host}:{self.root_port}/send_event", json=message)
                    
                    except httpx.ConnectError as e:
                        self.logger.error(f"Could not connect to root server at {self.root_host}:{self.root_port} - {str(e)}")
                        self.queue.put(message)

                    except Exception as e:
                        self.logger.error(f"Error forwarding message: {str(e)}")
                        self.queue.put(message)
                    
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

            # Connect RPC servers to RPC clients
            task = []
            for node in self.pipeline.nodes:
                task.append(node.node_server.connect())
            
            responses = await asyncio.gather(*task)

            # Finish the connection process for each leaf pipeline
            # Leaf pipeline will set the connection event for each node
            task = []
            for leaf_ip_address in self.leaf_ip_addresses:
                task.append(client.post(f"{leaf_ip_address}/finish_connect"))

            responses = await asyncio.gather(*task)
        
    def frontend_json(self):
        leaf_data = {}
        leaf_data["nodes"] = self.pipeline.model().model_dump()["nodes"]

        edges = []
        for leaf_node_model, leaf_node in zip(leaf_data["nodes"], self.pipeline.nodes):
            subapp: BaseGUI = leaf_node.get_node_gui()
            leaf_node_model["id"] = leaf_node_model["name"]
            leaf_node_model["label"] = leaf_node_model["name"]
            leaf_node_model["origin_url"] = f"http://{self.host}:{self.port}"
            leaf_node_model["type"] = type(leaf_node).__name__
            leaf_node_model["endpoint"] = f"http://{self.host}:{self.port}{subapp.get_endpoint()}"
            leaf_node_model["status_endpoint"] = f"http://{self.host}:{self.port}{subapp.get_status_endpoint()}"

            # add the remote successors to the node model
            for remote_successor_url in leaf_node.remote_successors:
                parsed = urlparse(remote_successor_url)
                successor_name = parsed.path.split("/")[-1]
                leaf_node_model["successors"].append(successor_name)

            edges_from_node = [
                {"source": leaf_node_model["id"], "target": successor} for successor in leaf_node_model["successors"]
            ]
            edges.extend(edges_from_node)

        # add the leaf nodes to the model
        for leaf_graph_model in self.leaf_models:
            for leaf_node_model in leaf_graph_model["nodes"]:
                leaf_node_model["header_bar_endpoint"] = f'/header_bar/?node_id={leaf_node_model["id"]}'

            leaf_data["nodes"].extend(leaf_graph_model["nodes"])
            edges.extend(leaf_graph_model["edges"])

        leaf_data["edges"] = edges
        return leaf_data

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