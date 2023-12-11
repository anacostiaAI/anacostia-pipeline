import os
import sys
import json
import signal
from importlib import metadata
from multiprocessing import Process
from web3 import Web3

from jinja2.filters import FILTERS
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

import uvicorn

from .engine.base import NodeModel, BaseMetadataStoreNode, BaseResourceNode, BaseActionNode
from .engine.pipeline import Pipeline, PipelineModel


# Set up Web3 connection
NODE_URL = "https://patient-wispy-fog.ethereum-sepolia.quiknode.pro/1f33ba5d6b770d91559dfb2e74d81722ea3eaddf/"

# Contract details
contract_address = '0xb87E89d174Be71642475D4612Cf9dF19863888b8'
contract_abi = [
	{
		"inputs": [],
		"stateMutability": "nonpayable",
		"type": "constructor"
	},
	{
		"anonymous": False,
		"inputs": [
			{
				"indexed": True,
				"internalType": "bytes32",
				"name": "id",
				"type": "bytes32"
			}
		],
		"name": "ChainlinkCancelled",
		"type": "event"
	},
	{
		"anonymous": False,
		"inputs": [
			{
				"indexed": True,
				"internalType": "bytes32",
				"name": "id",
				"type": "bytes32"
			}
		],
		"name": "ChainlinkFulfilled",
		"type": "event"
	},
	{
		"anonymous": False,
		"inputs": [
			{
				"indexed": True,
				"internalType": "bytes32",
				"name": "id",
				"type": "bytes32"
			}
		],
		"name": "ChainlinkRequested",
		"type": "event"
	},
	{
		"anonymous": False,
		"inputs": [
			{
				"indexed": True,
				"internalType": "address",
				"name": "from",
				"type": "address"
			},
			{
				"indexed": True,
				"internalType": "address",
				"name": "to",
				"type": "address"
			}
		],
		"name": "OwnershipTransferRequested",
		"type": "event"
	},
	{
		"anonymous": False,
		"inputs": [
			{
				"indexed": True,
				"internalType": "address",
				"name": "from",
				"type": "address"
			},
			{
				"indexed": True,
				"internalType": "address",
				"name": "to",
				"type": "address"
			}
		],
		"name": "OwnershipTransferred",
		"type": "event"
	},
	{
		"anonymous": False,
		"inputs": [
			{
				"indexed": False,
				"internalType": "uint256",
				"name": "priceInEth",
				"type": "uint256"
			},
			{
				"indexed": False,
				"internalType": "uint256",
				"name": "priceInUsd",
				"type": "uint256"
			}
		],
		"name": "PriceUpdated",
		"type": "event"
	},
	{
		"anonymous": False,
		"inputs": [
			{
				"indexed": True,
				"internalType": "bytes32",
				"name": "requestId",
				"type": "bytes32"
			},
			{
				"indexed": False,
				"internalType": "uint256",
				"name": "accuracy",
				"type": "uint256"
			}
		],
		"name": "RequestFirstId",
		"type": "event"
	},
	{
		"inputs": [],
		"name": "acceptOwnership",
		"outputs": [],
		"stateMutability": "nonpayable",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "accuracy",
		"outputs": [
			{
				"internalType": "uint256",
				"name": "",
				"type": "uint256"
			}
		],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [
			{
				"internalType": "bytes32",
				"name": "_requestId",
				"type": "bytes32"
			},
			{
				"internalType": "uint256",
				"name": "_accuracy",
				"type": "uint256"
			}
		],
		"name": "fulfill",
		"outputs": [],
		"stateMutability": "nonpayable",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "getPriceInUsd",
		"outputs": [
			{
				"internalType": "uint256",
				"name": "",
				"type": "uint256"
			}
		],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "owner",
		"outputs": [
			{
				"internalType": "address",
				"name": "",
				"type": "address"
			}
		],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "priceInEth",
		"outputs": [
			{
				"internalType": "uint256",
				"name": "",
				"type": "uint256"
			}
		],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "priceInUsd",
		"outputs": [
			{
				"internalType": "uint256",
				"name": "",
				"type": "uint256"
			}
		],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "requestFirstId",
		"outputs": [
			{
				"internalType": "bytes32",
				"name": "requestId",
				"type": "bytes32"
			}
		],
		"stateMutability": "nonpayable",
		"type": "function"
	},
	{
		"inputs": [
			{
				"internalType": "address",
				"name": "to",
				"type": "address"
			}
		],
		"name": "transferOwnership",
		"outputs": [],
		"stateMutability": "nonpayable",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "withdrawLink",
		"outputs": [],
		"stateMutability": "nonpayable",
		"type": "function"
	}
]


PACKAGE_NAME = "anacostia_pipeline"
PACKAGE_DIR = os.path.dirname(sys.modules[PACKAGE_NAME].__file__)

# Additional Filters for Jinja Templates
def basename(value, attribute=None):
    return os.path.basename(value)

def type_name(value):
    return type(value).__name__

FILTERS['basename'] = basename
FILTERS['type'] = type_name

class Webserver:
    def __init__(self, p:Pipeline):
        self.p = p
        self.static_dir = os.path.join(PACKAGE_DIR, "static")
        self.templates_dir = os.path.join(PACKAGE_DIR, "templates")

          # Setup Web3
        self.w3 = Web3(Web3.HTTPProvider(NODE_URL))

            # Check if the connection is successful
        if not self.w3.is_connected():
            print("Failed to connect to Ethereum node.")
            return
        
        self.contract = self.w3.eth.contract(address=contract_address, abi=contract_abi)

        # Only used for self.run(blocking=False)
        self._proc = None

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
        def _metadata():
            data_uri = self.p.metadata_store.uri
            with open(data_uri, 'r') as f:
                data = json.load(f)
            return data

        @self.app.get('/', response_class=HTMLResponse)
        async def index(request: Request):
            nodes = self.p.model().nodes
            return self.templates.TemplateResponse("base.html", {"request": request, "nodes": nodes, "title": "Anacostia Pipeline"})

        
        def get_usd_price(self):
            try:
                return self.contract.functions.getPriceInUsd().call()
            except Exception as e:
                print(f"Error fetching USD price: {e}")
                return None

        def get_eth_price(self):
            try:
                return self.contract.functions.priceInEth().call()
            except Exception as e:
                print(f"Error fetching ETH price: {e}")
                return None

        @self.app.get('/price/usd')
        def fetch_usd_price():
                return {"usd_price": self.get_usd_price()}

        @self.app.get('/price/eth')
        def fetch_eth_price():
                return {"eth_price": self.get_eth_price()}

        @self.app.get('/node', response_class=HTMLResponse)
        async def node_html(request: Request, name:str, property:str=None):
            node = self.p[name]
            if node is None:
                return "<h1>Node Not Found</h1>"
            
            if property is not None:
                n = node.model().dict()
                return n.get(property, "")

            # TODO update this so all node types have a .model() that
            # returns a pydantic BaseModel Type to streamlize serialization of nodes
            if isinstance(node, BaseMetadataStoreNode):
                return node.html(self.templates, request)

            if isinstance(node, BaseResourceNode):
                return node.html(self.templates, request)

            if isinstance(node, BaseActionNode):
                return node.html(self.templates, request)

            n = node.model()
            return n.view(self.templates, request)

    def run(self, blocking=False):
        '''
        Launches the Webserver (Blocking Call)
        '''
        if blocking:
            uvicorn.run(self.app, host="0.0.0.0", port=8000)
            return None
        
        # Launch webserver as new daemon process 
        def _uvicorn_wrapper():
            uvicorn.run(self.app, host="0.0.0.0", port=8000)
        
        print("Launching Webserver")
        self._proc = Process(target=_uvicorn_wrapper, args=())
        self._proc.start()
        print(f"Webserver PID: {self._proc.pid}")
        
        # create handler from this process to kill self._proc
        def _kill_handler(sig, frame):
            print("CTRL+C Caught!; Killing Webserver...")
            # webserver also catches a sigint causing it to die too
            self._proc.join()
            
        signal.signal(signal.SIGINT, _kill_handler)
        print("CTRL+C to Kill the Webserver; Or send a SIGINT to THIS process")
        return self._proc.pid