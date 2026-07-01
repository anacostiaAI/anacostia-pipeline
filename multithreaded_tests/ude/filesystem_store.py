from typing import List, Dict, Tuple, Union
from fastapi.routing import APIRoute
from fastapi import Request
from fastapi.responses import HTMLResponse, StreamingResponse
import asyncio

from anacostia_pipeline.nodes.resources.filesystem.gui import FilesystemStoreGUI
from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.resources.filesystem.fragments import create_table_rows, table_row
from anacostia_pipeline.utils.sse import format_html_for_sse

from metadata_store import UDEMetadataStoreNode
from fragments import ude_head_template, ude_filesystemstore_home



class UDEFilesystemStoreGUI(BaseGUI):
    def __init__(
        self, node, host: str, port: int, 
        metadata_store: UDEMetadataStoreNode, 
        ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(
            node=node, host=host, port=port, 
            metadata_store=metadata_store, 
            ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, 
            *args, **kwargs
        )

        self.metadata_store = metadata_store

        self.event_source = f"{self.get_node_prefix(remove_beginning_slash=True)}/table_update_events"
        self.event_name = "TableUpdate"

        self.displayed_file_entries = None

        def format_file_entries(file_entries: Union[List[Dict], Dict]) -> Union[List[Dict], Dict]:
            # adding on file_display_endpoint to each entry to get the contents of the file when user clicks on row 
            # note: state_change_event_name is used to update the state of the file entry via SSEs
            if type(file_entries) is list:
                for file_entry in file_entries:
                    file_entry['created_at'] = file_entry['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                    file_entry["file_display_endpoint"] = f"{self.get_node_prefix(remove_beginning_slash=True)}/retrieve_file?file_id={file_entry['id']}"
                    file_entry["state_change_event_name"] = f"StateUpdate{file_entry['id']}"
                return file_entries

            elif type(file_entries) is dict:
                file_entry = file_entries
                file_entry['created_at'] = file_entry['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                file_entry["file_display_endpoint"] = f"{self.get_node_prefix(remove_beginning_slash=True)}/retrieve_file?file_id={file_entry['id']}"
                file_entry["state_change_event_name"] = f"StateUpdate{file_entry['id']}"
                return file_entry

        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            if self.metadata_store is not None:
                file_entries = self.metadata_store.get_entries(resource_node_name=self.node.name)
            else:
                if self.metadata_store_client is not None:
                    file_entries = self.metadata_store_client.get_entries(resource_node_name=self.node.name)

            self.displayed_file_entries = file_entries
            file_entries.reverse()
            file_entries = format_file_entries(file_entries)

            return self.home(
                file_entries = file_entries,
                request=request
            ) 

        @self.get("/table_update_events", response_class=HTMLResponse)
        async def samples(request: Request):

            async def get_table_update_events() -> Tuple[List[Dict]]:
                file_entries = None
                if self.metadata_store is not None:
                    file_entries = self.metadata_store.get_entries(resource_node_name=self.node.name)
                else:
                    if self.metadata_store_client is not None:
                        file_entries = self.metadata_store_client.get_entries(resource_node_name=self.node.name)

                added_rows = []
                entry_ids = [displayed_entry["id"] for displayed_entry in self.displayed_file_entries]
                for retrieved_entry in file_entries:
                    if retrieved_entry["id"] not in entry_ids:
                        added_rows.append(retrieved_entry)

                state_changes = []
                for displayed_entry, retrieved_entry in zip(self.displayed_file_entries, file_entries):
                    if displayed_entry["state"] != retrieved_entry["state"]:
                        state_changes.append(retrieved_entry)
                
                self.displayed_file_entries = file_entries
                
                return added_rows, state_changes
            
            async def event_stream():
                print("event source /table_update_events connected")
                while True:
                    try:
                        added_rows, state_changes = await get_table_update_events()

                        if len(added_rows) > 0: 
                            formatted_dict = format_file_entries(added_rows)        # add information into dictionaries to prepare for html conversion
                            html_snippet = create_table_rows(formatted_dict)        # convert dictionaries to html
                            sse_message = format_html_for_sse(html_snippet)         # convert html to SSE message

                            yield "event: TableUpdate\n"
                            yield sse_message
                        
                        if len(state_changes) > 0:
                            for file_entry in state_changes:
                                formatted_dict = format_file_entries(file_entry)    # add information into dictionary to prepare for html conversion
                                html_snippet = table_row(formatted_dict)            # convert dictionary to html
                                sse_message = format_html_for_sse(html_snippet)     # convert html to SSE message 

                                yield f"event: StateUpdate{file_entry['id']}\n"
                                yield sse_message
                        
                        await asyncio.sleep(0.2)

                    except asyncio.CancelledError:
                        print("event source /table_update_events closed")
                        break 

                    except Exception as e:
                        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
                        break 

            return StreamingResponse(event_stream(), media_type="text/event-stream")

    def home(self, file_entries, request: Request):
        return ude_filesystemstore_home(
            header_template=ude_head_template(
                '''
                <!-- custom CSS for tables -->
                <link hx-head="re-eval" rel="stylesheet" type="text/css" href="static/css/styles/tables.css">
                '''
            ),
            sse_endpoint=self.event_source,
            event_name = self.event_name,
            file_entries = file_entries 
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


# Override the FilesystemStoreNode to create a custom data store node that uses the custom GUI.
class UDEFilesystemStoreNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: UDEMetadataStoreNode) -> None:
        super().__init__(name=name, resource_path=resource_path, metadata_store=metadata_store)
    
    def setup_node_GUI(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None):
        self.gui = UDEFilesystemStoreGUI(
            node=self, host=host, port=port, 
            metadata_store=self.metadata_store, 
            ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs
        )
        return self.gui
