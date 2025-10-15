from fastapi import Request
from fastapi.responses import HTMLResponse

from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.resources.filesystem.croissant.fragments import dataset_registry_home



class DatasetRegistryGUI(BaseGUI):
    def __init__(
        self, node, 
        host: str, port: int, 
        metadata_store = None, metadata_store_client = None, 
        ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None,
        *args, **kwargs
    ):
        super().__init__(node, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, *args, **kwargs)

        if metadata_store is None and metadata_store_client is None:
            raise ValueError("Either metadata_store or metadata_store_rpc must be provided")

        self.metadata_store = metadata_store
        self.metadata_store_client = metadata_store_client
        
        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            return dataset_registry_home(
                update_endpoint=f"{self.get_gui_url()}/update_home_page"
            ) 
        
        @self.get("/update_home_page", response_class=HTMLResponse)
        async def update_home_page(request: Request):
            return "<div>No model entries found</div>"
        