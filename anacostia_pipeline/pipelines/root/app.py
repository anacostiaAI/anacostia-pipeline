import threading
import signal
import asyncio
from logging import Logger
from contextlib import asynccontextmanager
import os
import sys
from queue import Queue
import json

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.routing import Mount

import uvicorn
import httpx

from anacostia_pipeline.nodes.app import BaseApp
from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.nodes.network.sender.node import SenderNode
from anacostia_pipeline.pipelines.utils import EventModel
from anacostia_pipeline.pipelines.root.fragments import node_bar_closed, node_bar_open, node_bar_invisible, index_template



class RootPipelineApp(FastAPI):
    def __init__(self, name: str, pipeline: RootPipeline, host: str = "localhost", port: int = 8000, logger: Logger = None, *args, **kwargs):

        # lifespan context manager for spinning up and shutting down the service
        @asynccontextmanager
        async def lifespan(app: RootPipelineApp):
            app.logger.info(f"Opening client for service '{app.name}'")

            yield
            
            for route in app.routes:
                if isinstance(route, Mount) and isinstance(route.app, BaseApp):
                    subapp: BaseApp = route.app
                    # this currently does nothing, add code here for cleanup if needed

            app.logger.info(f"Closing client for service '{app.name}'")
            # Note: we need to close the client after the lifespan context manager is done but for some reason await app.client.aclose() is throwing an error 
            # RuntimeError: unable to perform operation on <TCPTransport closed=True reading=False 0x121fa0fd0>; the handler is close
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.host = host
        self.port = port
        self.pipeline = pipeline
        self.logger = logger
        self.connections = []
        self.leaf_ip_addresses = []
        self.leaf_configs = []
        self.client = httpx.AsyncClient()
        self.queue = Queue()

        # Mount the static files directory to the webserver
        DASHBOARD_DIR = os.path.dirname(sys.modules["anacostia_pipeline"].__file__)
        self.static_dir = os.path.join(DASHBOARD_DIR, "static")
        self.mount("/static", StaticFiles(directory=self.static_dir), name="webserver")

        config = uvicorn.Config(self, host=self.host, port=self.port)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)
    
        # Extract data about leaf pipelines from the sender nodes in the pipeline
        for node in self.pipeline.nodes:
            if isinstance(node, SenderNode):
                connection_dict = {
                    "root_name": self.name,
                    "leaf_host": node.leaf_host,
                    "leaf_port": node.leaf_port,
                    "root_host": self.host,
                    "root_port": self.port,
                    "sender_name": node.name,
                    "receiver_name": node.leaf_receiver,
                }
                
                if any([connection_dict["receiver_name"] == connection["receiver_name"] for connection in self.connections]):
                    raise ValueError(f"Duplicate receiver name '{connection_dict['receiver_name']}' found in the pipeline")
                else:
                    self.connections.append(connection_dict)

            # Extract the leaf ip addresses from the connections
            for connection in self.connections:
                pipeline_ip_address = f"{connection['leaf_host']}:{connection['leaf_port']}"
                if pipeline_ip_address not in self.leaf_ip_addresses:
                    self.leaf_ip_addresses.append(f"{connection['leaf_host']}:{connection['leaf_port']}")
                    self.logger.info(f"Root service '{self.name}' beginning connection protocol to leaf services at ip addresses: {self.leaf_ip_addresses}")

        # Connect to the leaf services
        if len(self.leaf_ip_addresses) > 0:
            asyncio.run(self.connect())

        # Mount the apps from the pipeline nodes to the webserver
        for node in self.pipeline.nodes:
            node_subapp: BaseApp = node.get_app()
            self.mount(node_subapp.get_node_prefix(), node_subapp)          # mount the BaseNodeApp to PipelineWebserver
            node.set_queue(self.queue)                                      # set the queue for the node

        @self.get('/', response_class=HTMLResponse)
        async def index(request: Request):
            frontend_json = self.frontend_json()
            nodes = frontend_json["nodes"]
            return index_template(nodes, frontend_json, "/graph_sse")

        @self.get("/header_bar", response_class=HTMLResponse)
        def header_bar(node_id: str, visibility: bool = False):
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
        try:
            # Note: don't use httpx.post here, it will throw an error "object Response can't be used in 'await' expression"
            # Instead, we use await self.client.post because we already have an httpx.AsyncClient() object 
            # created in the lifespan context manager in the AnacostiaService class.
            # See this video: https://www.youtube.com/watch?v=row-SdNdHFE

            # Send a /healthcheck request to each leaf service
            self.logger.info("------------- Healthcheck started -------------")
            tasks = []
            for ip_address in self.leaf_ip_addresses:
                tasks.append(self.client.post(f"http://{ip_address}/healthcheck"))

            responses = await asyncio.gather(*tasks)

            for response in responses:
                response_data = response.json()
                if response_data["status"] == "ok":
                    self.logger.info(f"Successfully connected to leaf at {ip_address}")
            self.logger.info("------------- Healthcheck completed -------------")
                
            self.logger.info("------------- Leaf pipeline creation started -------------")
            # Send a /connect request to each leaf service and store the pipeline ID
            tasks = []
            for ip_address in self.leaf_ip_addresses:
                tasks.append(self.client.post(f"http://{ip_address}/connect", json=self.connections))
            
            responses = await asyncio.gather(*tasks)

            for response in responses:
                response_data = response.json()
                # self.logger.info(f"leaf data: {response_data}")
                self.leaf_configs.append(response_data)

            self.logger.info("------------- Leaf pipeline creation completed -------------")

        except Exception as e:
            print(f"Failed to connect to leaf at {ip_address} with error: {e}")
            self.logger.error(f"Failed to connect to leaf at {ip_address} with error: {e}")
            
    def frontend_json(self):
        model = self.pipeline.pipeline_model.model_dump()
        edges = []
        for node_model, node in zip(model["nodes"], self.pipeline.nodes):
            subapp = node.get_app()
            node_model["id"] = node_model["name"]
            node_model["label"] = node_model["name"]
            node_model["origin_url"] = f"http://{self.host}:{self.port}"
            node_model["type"] = type(node).__name__
            node_model["endpoint"] = f"http://{self.host}:{self.port}{subapp.get_endpoint()}"
            node_model["status_endpoint"] = f"http://{self.host}:{self.port}{subapp.get_status_endpoint()}"
            node_model["header_bar_endpoint"] = f'/header_bar/?node_id={node_model["id"]}'

            if isinstance(node, SenderNode):
                node_model["successors"].append(node.leaf_receiver)

            edges_from_node = [
                {"source": node_model["id"], "target": successor} for successor in node_model["successors"]
            ]
            edges.extend(edges_from_node)

        for leaf_config in self.leaf_configs:
            for leaf_data_node in leaf_config["nodes"]:
                leaf_data_node["header_bar_endpoint"] = f'/header_bar/?node_id={leaf_data_node["id"]}'

            model["nodes"].extend(leaf_config["nodes"])
            edges.extend(leaf_config["edges"])

        model["edges"] = edges
        return model

    def run(self):
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            print("Killing pipeline...")
            self.pipeline.terminate_nodes()
            print("Pipeline Killed.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        self.fastapi_thread.start()

        # Launch the root pipeline
        self.pipeline.launch_nodes()

        if sys.version_info.major == 3 and sys.version_info.minor >= 12:
            # keep the main thread open; this is done to avoid an error in python 3.12 "RuntimeError: can't create new thread at interpreter shutdown"
            for thread in threading.enumerate():
                if thread.daemon or thread is threading.current_thread():
                    continue
                thread.join()