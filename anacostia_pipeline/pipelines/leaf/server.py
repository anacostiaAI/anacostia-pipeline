import threading
import signal
from logging import Logger
from contextlib import asynccontextmanager
from queue import Queue
import asyncio
from pydantic import BaseModel
from typing import List, Dict, Any
import sys

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware

import uvicorn
import httpx

from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.connector import Connector
from anacostia_pipeline.nodes.rpc import BaseRPCCaller



class RootServerModel(BaseModel):
    root_host: str
    root_port: int



class LeafPipelineServer(FastAPI):
    def __init__(
        self, name: str, 
        pipeline: LeafPipeline, 
        rpc_callers: List[BaseRPCCaller],
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

        # Mount the apps and connectors to the webserver
        for node in self.pipeline.nodes:
            connector: Connector = node.setup_connector(host=self.host, port=self.port)
            self.mount(connector.get_connector_prefix(), connector)          

            node_gui: BaseGUI = node.setup_node_GUI(host=self.host, port=self.port)
            self.mount(node_gui.get_node_prefix(), node_gui)                # mount the BaseNodeApp to PipelineWebserver
            node.set_queue(self.queue)                                      # set the queue for the node
        
        for rpc_caller in rpc_callers:
            self.mount(rpc_caller.get_caller_prefix(), rpc_caller)          # mount the BaseRPCCaller to PipelineWebserver
            rpc_caller.add_loggers(self.logger)                             # add the logger to the rpc_caller
            rpc_caller.caller_host = self.host
            rpc_caller.caller_port = self.port

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

    def frontend_json(self):
        leaf_data = {}
        leaf_data["nodes"] = self.pipeline.model().model_dump()["nodes"]

        edges = []
        for leaf_data_node, leaf_node in zip(leaf_data["nodes"], self.pipeline.nodes):
            subapp: BaseGUI = leaf_node.get_node_gui()
            leaf_data_node["id"] = leaf_data_node["name"]
            leaf_data_node["label"] = leaf_data_node["name"]
            leaf_data_node["origin_url"] = f"http://{self.host}:{self.port}"
            leaf_data_node["type"] = type(leaf_node).__name__
            leaf_data_node["endpoint"] = f"http://{self.host}:{self.port}{subapp.get_endpoint()}"
            leaf_data_node["status_endpoint"] = f"http://{self.host}:{self.port}{subapp.get_status_endpoint()}"

            edges_from_node = [
                {"source": leaf_data_node["id"], "target": successor} for successor in leaf_data_node["successors"]
            ]
            edges.extend(edges_from_node)

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