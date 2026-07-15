from typing import List, Dict, Union
import json
import os

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute

from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.nodes.resources.filesystem.croissant.node import DatasetRegistryNode

from fragments import ude_head_template
from dataset_registry_fragments import dataset_registry_home, dataset_card_modal, dataset_card
from metadata_store import UDEMetadataStoreNode



class UDEDatasetRegistryGUI(BaseGUI):
    def __init__(
        self, node, 
        host: str, port: int, 
        metadata_store: BaseMetadataStoreNode = None, 
        metadata_store_client: BaseMetadataStoreClient = None, 
        ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None,
        *args, **kwargs
    ):
        super().__init__(node, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, *args, **kwargs)

        if metadata_store is None and metadata_store_client is None:
            raise ValueError("Either metadata_store or metadata_store_rpc must be provided")

        self.metadata_store = metadata_store
        self.metadata_store_client = metadata_store_client
        self.rendered_cards = []

        def render_file_entries(file_entries: Union[List[Dict], Dict], full_page_reload: bool = False) -> Union[List[Dict], Dict]:
            data_card_entries = [entry for entry in file_entries if entry["location"].endswith(".json") is True]

            entries_to_render = []

            for data_card_entry in data_card_entries:
                data_card_path = data_card_entry['location']

                if full_page_reload is True:
                    # on full page reload, render all data cards
                    data_card_fullpath = os.path.join(self.node.resource_path, data_card_path)
                    
                    entries_to_render.append(
                        dataset_card(
                            data_card_path=data_card_fullpath,
                            modal_open_endpoint=f"{self.get_node_prefix(remove_beginning_slash=True)}/modal/?action=open&card_path={data_card_path}"
                        )
                    )

                if data_card_path not in self.rendered_cards:
                    self.rendered_cards.append(data_card_path)
                    data_card_fullpath = os.path.join(self.node.resource_path, data_card_path)
                    
                    entries_to_render.append(
                        dataset_card(
                            data_card_path=data_card_fullpath,
                            modal_open_endpoint=f"{self.get_node_prefix(remove_beginning_slash=True)}/modal/?action=open&card_path={data_card_path}"
                        )
                    )
                
            return entries_to_render
        
        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            if self.metadata_store is not None:
                file_entries = self.metadata_store.get_entries(resource_node_name=self.node.name)
            else:
                if self.metadata_store_client is not None:
                    file_entries = self.metadata_store_client.get_entries(resource_node_name=self.node.name)

            data_card_entries = render_file_entries(file_entries, full_page_reload=True)

            return self.home(
                update_endpoint=f"{self.get_node_prefix(remove_beginning_slash=True)}/update_home_page",
                dataset_entries=data_card_entries
            )

        @self.get("/update_home_page", response_class=HTMLResponse)
        async def update_home_page(request: Request):
            if self.metadata_store is not None:
                file_entries = self.metadata_store.get_entries(resource_node_name=self.node.name)
            else:
                if self.metadata_store_client is not None:
                    file_entries = self.metadata_store_client.get_entries(resource_node_name=self.node.name)

            data_card_entries = render_file_entries(file_entries)
            data_card_entries_str = "\n".join(data_card_entries) 
            return data_card_entries_str
        
        @self.get("/modal/", response_class=HTMLResponse)
        async def modal(action: str, card_path: str = None):
            if action == "open":
                card_path = os.path.join(self.node.resource_path, card_path)

                with open(card_path, "r", encoding="utf-8") as file:
                    modal_html_str = json.load(file)
                    modal_html_str = json.dumps(modal_html_str, indent=4)

                    return dataset_card_modal(
                        modal_close_endpoint=f"{self.get_node_prefix(remove_beginning_slash=True)}/modal/?action=close",
                        modal_html_str=modal_html_str
                    )

            elif action == "close":
                return ""

    def home(self, update_endpoint: str, dataset_entries: List[str]) -> str:
        return dataset_registry_home(
            head_template=ude_head_template(
                '''
                <!-- CSS for model registry home page -->
                <link rel="stylesheet" href="static/css/styles/croissant.css">
    
                <!-- CSS for markdown modal -->
                <link rel="stylesheet" href="static/css/styles/markdown.css">
                '''
            ),
            update_endpoint=update_endpoint,
            dataset_entries=dataset_entries
        )

    def get_node_prefix(self, remove_beginning_slash: bool = False):
        if remove_beginning_slash:
            return f"{self.node.name}/hypermedia"
        else:
            return f"/{self.node.name}/hypermedia"

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.node.name}/hypermedia/home"
        else:
            return ''


class UDEDatasetRegistryNode(DatasetRegistryNode):
    def __init__(self, name: str, resource_path: str, metadata_store: UDEMetadataStoreNode) -> None:
        super().__init__(name=name, resource_path=resource_path, metadata_store=metadata_store)

    def setup_node_GUI(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None):
        self.gui = UDEDatasetRegistryGUI(
            node=self,
            host=host,
            port=port,
            metadata_store=self.metadata_store,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_ca_certs=ssl_ca_certs
        )
        return self.gui