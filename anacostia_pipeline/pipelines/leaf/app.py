import os
import sys
import httpx
import uuid
from logging import Logger

from fastapi import FastAPI

from anacostia_pipeline.nodes.network.receiver.app import ReceiverApp
from anacostia_pipeline.nodes.network.sender.app import SenderApp
from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.utils.constants import Work



PACKAGE_NAME = "anacostia_pipeline"
DASHBOARD_DIR = os.path.dirname(sys.modules["anacostia_pipeline"].__file__)



class LeafPipelineApp(FastAPI):
    def __init__(self, name: str, pipeline: LeafPipeline, host="127.0.0.1", port=8000, logger: Logger = None, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.name = name
        self.pipeline = pipeline
        self.host = host
        self.port = port
        self.client = httpx.AsyncClient()
        self.logger = logger
        self.pipeline_id = uuid.uuid4().hex      # figure out a way to merge this pipeline_id with the pipeline_id in LeafServiceApp

        for node in self.pipeline.nodes:
            node_subapp = node.get_app()

            if isinstance(node_subapp, ReceiverApp) or isinstance(node_subapp, SenderApp):
                node_subapp.set_leaf_pipeline_id(self.pipeline_id)

            self.mount(node_subapp.get_node_prefix(), node_subapp)       # mount the BaseNodeApp to PipelineWebserver
    
    def get_pipeline_id(self):
        return self.pipeline_id

    def run(self):
        print(f"launched pipeline {self.pipeline_id}")
        self.pipeline.launch_nodes()
    
    def shutdown(self):
        self.pipeline.terminate_nodes()