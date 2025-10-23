from typing import List, Dict, Union
import json

from fastapi import Request
from fastapi.responses import HTMLResponse

from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.nodes.resources.filesystem.croissant.fragments import dataset_registry_home, data_entry_card, data_card_modal



class DatasetRegistryGUI(BaseGUI):
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

        def render_file_entries(file_entries: Union[List[Dict], Dict]) -> Union[List[Dict], Dict]:
            data_card_entries = [entry for entry in file_entries if entry["location"].endswith(".json") is True]

            entries_to_render = []

            for data_card_entry in data_card_entries:
                data_card_path = data_card_entry['location']
                
                entries_to_render.append(
                    data_entry_card(
                        data_card_entry=data_card_entry, 
                        modal_open_endpoint=f"{self.get_gui_url()}/modal/?action=open&card_path={data_card_path}"
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

            file_entries.reverse()
            data_card_entries = render_file_entries(file_entries)

            return dataset_registry_home(
                update_endpoint=f"{self.get_gui_url()}/update_home_page",
                dataset_entries=data_card_entries
            )

        @self.get("/update_home_page", response_class=HTMLResponse)
        async def update_home_page(request: Request):
            if self.metadata_store is not None:
                file_entries = self.metadata_store.get_entries(resource_node_name=self.node.name)
            else:
                if self.metadata_store_client is not None:
                    file_entries = self.metadata_store_client.get_entries(resource_node_name=self.node.name)

            file_entries.reverse()
            data_card_entries = render_file_entries(file_entries)
            data_card_entries_str = "\n".join(data_card_entries) 
            return data_card_entries_str
        
        @self.get("/modal/", response_class=HTMLResponse)
        async def modal(action: str, card_path: str = None):
            if action == "open":
                card_path = f"{self.node.resource_path}/{card_path}"

                with open(card_path, "r", encoding="utf-8") as file:
                    modal_html_str = json.load(file)
                    modal_html_str=json.dumps(modal_html_str, indent=4)

                    return data_card_modal(
                        modal_close_endpoint=f"{self.get_gui_url()}/modal/?action=close",
                        modal_html_str=modal_html_str
                    )

            elif action == "close":
                return ""