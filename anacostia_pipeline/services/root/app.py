import threading
import signal
import asyncio
import uuid
from logging import Logger
from contextlib import asynccontextmanager
from typing import List, Dict
import os
import sys

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.routing import Mount

import uvicorn
import httpx
from pydantic import BaseModel

from anacostia_pipeline.nodes.node import NodeModel
from anacostia_pipeline.nodes.app import BaseApp
from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.nodes.network.receiver.app import ReceiverApp
from anacostia_pipeline.nodes.network.sender.node import SenderNode

from anacostia_pipeline.utils.constants import Work

from anacostia_pipeline.services.root.fragments import node_bar_closed, node_bar_open, node_bar_invisible, index_template


class RootServiceData(BaseModel):
    root_name: str
    leaf_host: str
    leaf_port: int
    root_host: str
    root_port: int
    sender_name: str
    receiver_name: str
    pipeline_id: str



class RootServiceApp(FastAPI):
    def __init__(self, name: str, pipeline: RootPipeline, host: str = "localhost", port: int = 8000, logger: Logger = None, *args, **kwargs):

        # lifespan context manager for spinning up and shutting down the service
        @asynccontextmanager
        async def lifespan(app: RootServiceApp):
            app.log(f"Opening client for service '{app.name}'")

            yield

            app.log(f"Closing client for service '{app.name}'")
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
        self.client = httpx.AsyncClient()

        # Mount the static files directory to the webserver
        DASHBOARD_DIR = os.path.dirname(sys.modules["anacostia_pipeline"].__file__)
        self.static_dir = os.path.join(DASHBOARD_DIR, "static")
        self.mount("/static", StaticFiles(directory=self.static_dir), name="webserver")

        config = uvicorn.Config(self, host=self.host, port=self.port)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)
    
        # Connect to the leaf services
        asyncio.run(self.connect())

        # Mount the apps from the pipeline nodes to the webserver
        for node in self.pipeline.nodes:
            node_subapp: BaseApp = node.get_app()
            self.mount(node_subapp.get_node_prefix(), node_subapp)       # mount the BaseNodeApp to PipelineWebserver

        @self.get('/', response_class=HTMLResponse)
        async def index(request: Request):
            frontend_json = self.__frontend_json()
            nodes = frontend_json["nodes"]
            return index_template(nodes, frontend_json, "/graph_sse")

        @self.get("/header_bar", response_class=HTMLResponse)
        def header_bar(node_id: str, visibility: bool = False):
            html_responses = []
            frontend_json = self.__frontend_json()

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
            
        @self.get('/graph_sse', response_class=StreamingResponse)
        async def graph_sse(request: Request):
            edge_color_table = {}
            for node in self.pipeline.nodes:
                for successor in node.successors:
                    edge_color_table[f"{node.name}_{successor.name}"] = None

            async def event_stream():
                while True:
                    try:
                        for node in self.pipeline.nodes:
                            for successor in node.successors:
                                edge_name = f"{node.name}_{successor.name}"

                                if Work.WAITING_SUCCESSORS in node.work_list:
                                    if edge_color_table[edge_name] != "red":
                                        yield f"event: {edge_name}_change_edge_color\n"
                                        yield f"data: red\n\n"
                                        edge_color_table[edge_name] = "red"
                                else: 
                                    if edge_color_table[edge_name] != "black":
                                        yield f"event: {edge_name}_change_edge_color\n"
                                        yield f"data: black\n\n"
                                        edge_color_table[edge_name] = "black"
                                
                        await asyncio.sleep(0.1)

                    except asyncio.CancelledError:
                        print("event source /graph_sse closed")
                        yield "event: close\n"
                        yield "data: \n\n"
                        break

            return StreamingResponse(event_stream(), media_type="text/event-stream")

    def log(self, message: str, level: str = "INFO"):
        if self.logger is not None:
            if level == "DEBUG":
                self.logger.debug(message)
            elif level == "INFO":
                self.logger.info(message)
            elif level == "WARNING":
                self.logger.warning(message)
            elif level == "ERROR":
                self.logger.error(message)
            elif level == "CRITICAL":
                self.logger.critical(message)
            else:
                raise ValueError(f"Invalid log level: {level}")
        else:
            print(f"{level}: {message}")
    
    async def connect(self):
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
                    "pipeline_id": ""
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
                    self.log(f"Root service '{self.name}' beginning connection protocol to leaf services at ip addresses: {self.leaf_ip_addresses}")

        try:
            # Note: don't use httpx.post here, it will throw an error "object Response can't be used in 'await' expression"
            # Instead, we use await self.client.post because we already have an httpx.AsyncClient() object 
            # created in the lifespan context manager in the AnacostiaService class.
            # See this video: https://www.youtube.com/watch?v=row-SdNdHFE

            # Send a /healthcheck request to each leaf service
            self.log("------------- Healthcheck started -------------")
            tasks = []
            for ip_address in self.leaf_ip_addresses:
                tasks.append(self.client.post(f"http://{ip_address}/healthcheck"))

            responses = await asyncio.gather(*tasks)

            for response in responses:
                response_data = response.json()
                if response_data["status"] == "ok":
                    self.log(f"Successfully connected to leaf at {ip_address}")
            self.log("------------- Healthcheck completed -------------")
                
            self.log("------------- Leaf pipeline creation started -------------")
            # Send a /create_pipeline request to each leaf service and store the pipeline ID
            tasks = []
            for ip_address in self.leaf_ip_addresses:
                tasks.append(self.client.post(f"http://{ip_address}/create_pipeline", json=self.connections))
            
            responses = await asyncio.gather(*tasks)

            for response in responses:
                response_data = response.json()
                self.log(f"leaf data: {response_data}")
                pipeline_id = response_data["pipeline_id"]

                for node in self.pipeline.nodes:
                    if isinstance(node, SenderNode):
                        if f"{node.leaf_host}:{node.leaf_port}" == ip_address:
                            node.get_app().set_leaf_pipeline_id(pipeline_id)

                            for connection in self.connections:
                                if connection["sender_name"] == node.name:
                                    connection["pipeline_id"] = pipeline_id
            self.log("------------- Leaf pipeline creation completed -------------")

        except Exception as e:
            print(f"Failed to connect to leaf at {ip_address} with error: {e}")
            self.logger.error(f"Failed to connect to leaf at {ip_address} with error: {e}")
            
    def __frontend_json(self):
        model = self.pipeline.pipeline_model.model_dump()
        edges = []
        for node_model, node in zip(model["nodes"], self.pipeline.nodes):
            node_model["id"] = node_model["name"]
            # label is for creating a more readable name, in the future, enable users to input their own labels
            node_model["label"] = node_model["name"].replace("_", " ")

            subapp = node.get_app()
            node_model["endpoint"] = f"http://{self.host}:{self.port}{subapp.get_endpoint()}"
            node_model["status_endpoint"] = f"http://{self.host}:{self.port}{subapp.get_status_endpoint()}"
            node_model["work_endpoint"] = f"http://{self.host}:{self.port}{subapp.get_work_endpoint()}"
            node_model["header_bar_endpoint"] = f'''/header_bar/?node_id={node_model["id"]}'''

            edges_from_node = [
                { 
                    "source": node_model["id"], "target": successor, 
                    "event_name": f"{node_model['id']}_{successor}_change_edge_color" 
                } 
                for successor in node_model["successors"]
            ]
            edges.extend(edges_from_node)

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
