from importlib import metadata

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request, Response

from engine.base import BaseNode, BaseMetadataStoreNode, NodeModel
from engine.pipeline import Pipeline, PipelineModel

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
    nodes = [
        {"name": "preprocessing", "endpoint": "/preprocessing"},
        {"name": "training", "endpoint": "/training"},
        {"name": "evaluation", "endpoint": "/evaluation"},
    ]
    return templates.TemplateResponse("base.html", {"request": request, "nodes": nodes, "title": "Anacostia Pipeline"})

@app.get("/preprocessing", response_class=HTMLResponse)
def preprocessing():
    return '<h1 class="blue-text">preprocessing</h1>'

@app.get("/training", response_class=HTMLResponse)
def training():
    return "<h1>training</h1>"

@app.get("/evaluation", response_class=HTMLResponse)
def evaluation():
    return "<h1>evaluation</h1>"