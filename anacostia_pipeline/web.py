from importlib import metadata

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

from anacostia_pipeline.engine.base import BaseNode, BaseMetadataStoreNode, NodeModel
from anacostia_pipeline.engine.pipeline import Pipeline, PipelineModel

PACKAGE_NAME="anacostia_pipeline"

app = FastAPI()
app.mount("/static", StaticFiles(directory="./static"), name="static")
templates = Jinja2Templates(directory="./templates")


# For now, Assuming we are running 1 Pipeline
n1=BaseMetadataStoreNode("test_node1")
n2=BaseNode("test_node2", predecessors=[n1])
p = Pipeline([n1, n2])

@app.get('/api')
def welcome():
    return {
        "package": PACKAGE_NAME,
        "version": metadata.version(PACKAGE_NAME)
    }

@app.get('/api/pipeline/')
def pipeline() -> PipelineModel:
    '''
    Returns information on the pipeline
    '''
    return p.model()


@app.get('/api/node/')
def nodes(name: str) -> NodeModel:
    '''
    Returns information on a given Node by name
    '''
    return p[name].model()

@app.get('/', response_class=HTMLResponse)
async def index(request: Request):
    nodes = p.model().nodes
    for n in nodes:
        print(n.endpoint())
    return templates.TemplateResponse("base.html", {"request": request, "nodes": nodes, "title": "Anacostia Pipeline"})

@app.get('/node', response_class=HTMLResponse)
async def node(request: Request, name:str, property:str=None):
    n = p[name].model()
    if n is None:
        return "<h1>Node Not Found</h1>"
    
    if property is None:
        return n.view(templates, request)

    return str(getattr(n, property))