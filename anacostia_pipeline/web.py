import os
import sys
from importlib import metadata
import json

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
import uvicorn

from .engine.base import NodeModel, BaseMetadataStoreNode, BaseResourceNode, BaseActionNode
from .engine.pipeline import Pipeline, PipelineModel

PACKAGE_NAME = "anacostia_pipeline"
PACKAGE_DIR = os.path.dirname(sys.modules[PACKAGE_NAME].__file__)

class Webserver:
    def __init__(self, p:Pipeline):
        self.p = p
        self.static_dir = os.path.join(PACKAGE_DIR, "static")
        self.templates_dir = os.path.join(PACKAGE_DIR, "templates")

        self.app = FastAPI()
        self.app.mount("/static", StaticFiles(directory=self.static_dir), name="static")
        self.templates = Jinja2Templates(directory=self.templates_dir)

        @self.app.get('/api')
        def welcome():
            return {
                "package": PACKAGE_NAME,
                "version": metadata.version(PACKAGE_NAME)
            }

        @self.app.get('/api/pipeline/')
        def pipeline() -> PipelineModel:
            '''
            Returns information on the pipeline
            '''
            return self.p.model()


        @self.app.get('/api/node/')
        def nodes(name: str) -> NodeModel:
            '''
            Returns information on a given Node by name
            '''
            return self.p[name].model()

        @self.app.get('/api/metadata')
        def metadata():
            data_uri = self.p.metadata_store.uri
            with open(data_uri, 'r') as f:
                data = json.load(f)
            return data

        @self.app.get('/', response_class=HTMLResponse)
        async def index(request: Request):
            nodes = self.p.model().nodes
            return self.templates.TemplateResponse("base.html", {"request": request, "nodes": nodes, "title": "Anacostia Pipeline"})

        @self.app.get('/node', response_class=HTMLResponse)
        async def node_html(request: Request, name:str, property:str=None):
            node = self.p[name]
            if node is None:
                return "<h1>Node Not Found</h1>"
            
            # TODO update this so all node types have a .model() that
            # returns a pydantic BaseModel Type to streamlize serialization of nodes
            if isinstance(node, BaseMetadataStoreNode):
                return node.html(self.templates, request)

            if isinstance(node, BaseResourceNode):
                return "BaseResourceNode"

            if isinstance(node, BaseActionNode):
                return "BaseActionNode"

            n = node.model()
            
            if property is None:
                return n.view(self.templates, request)

            return str(getattr(n, property))

    def run(self):
        '''
        Launches the Webserver (Blocking Call)
        '''
        uvicorn.run(self.app, host="0.0.0.0", port=8000)