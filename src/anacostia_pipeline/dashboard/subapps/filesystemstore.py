from fastapi import Request
from fastapi.responses import HTMLResponse, StreamingResponse
import asyncio
from typing import List, Dict, Tuple, Union

from anacostia_pipeline.dashboard.subapps.basenode import BaseNodeApp
from ..components.filesystemstore import filesystemstore_home, filesystemstore_viewer, create_table_rows, table_row
from ..components.utils import format_html_for_sse



class FilesystemStoreNodeApp(BaseNodeApp):
    def __init__(self, node, use_default_file_renderer: str = True, *args, **kwargs):
        super().__init__(node, use_default_router=False, *args, **kwargs)

        self.event_source = f"{self.get_prefix()}/table_update_events"
        self.event_name = "TableUpdate"

        self.displayed_file_entries = None

        def format_file_entries(file_entries: Union[List[Dict], Dict]) -> Union[List[Dict], Dict]:
            # adding on file_display_endpoint to each entry to get the contents of the file when user clicks on row 
            # note: state_change_event_name is used to update the state of the file entry via SSEs
            if type(file_entries) is list:
                for file_entry in file_entries:
                    file_entry['created_at'] = file_entry['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                    file_entry["file_display_endpoint"] = f"{self.get_prefix()}/retrieve_file?file_id={file_entry['id']}"
                    file_entry["state_change_event_name"] = f"StateUpdate{file_entry['id']}"
                    if file_entry['end_time'] is not None:
                        file_entry['end_time'] = file_entry['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
                return file_entries

            elif type(file_entries) is dict:
                file_entry = file_entries
                file_entry['created_at'] = file_entry['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                file_entry["file_display_endpoint"] = f"{self.get_prefix()}/retrieve_file?file_id={file_entry['id']}"
                file_entry["state_change_event_name"] = f"StateUpdate{file_entry['id']}"
                if file_entry['end_time'] is not None:
                    file_entry['end_time'] = file_entry['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
                return file_entry

        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            file_entries = self.node.metadata_store.get_entries()
            self.displayed_file_entries = file_entries
            file_entries.reverse()
            file_entries = format_file_entries(file_entries)

            return filesystemstore_home(
                sse_endpoint = self.event_source,
                event_name = self.event_name,
                file_entries = file_entries,
            ) 

        @self.get("/table_update_events", response_class=HTMLResponse)
        async def samples(request: Request):

            def get_table_update_events() -> Tuple[List[Dict]]:
                file_entries = self.node.metadata_store.get_entries()

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
                        added_rows, state_changes = get_table_update_events()

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

        
        if use_default_file_renderer:
            @self.get("/retrieve_file", response_class=HTMLResponse)
            async def sample(request: Request, file_id: int):
                artifact_path = self.node.get_artifact(file_id)["location"]
                content = self.node.load_artifact(artifact_path)
                x = filesystemstore_viewer(content, f"Content of {artifact_path}")
                return x
    