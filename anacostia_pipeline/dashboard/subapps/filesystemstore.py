from fastapi import Request
from fastapi.responses import HTMLResponse, StreamingResponse
import asyncio
from typing import List, Dict

from anacostia_pipeline.dashboard.subapps.basenode import BaseNodeApp
from ..components.filesystemstore import filesystemstore_home, filesystemstore_viewer, create_table_rows
from ..components.utils import format_html_for_sse
import time



class FilesystemStoreNodeApp(BaseNodeApp):
    def __init__(self, node, use_default_file_renderer: str = True, *args, **kwargs):
        super().__init__(
            node, 
            '<link rel="stylesheet" type="text/css" href="/static/css/filesystemstore.css">',
            use_default_router=False, *args, **kwargs
        )

        self.event_source = f"{self.get_prefix()}/table_update_events"
        self.event_name = "TableUpdate"

        def get_table_update_events() -> List[Dict]:
            # retrieving file ID, run ID, node ID (but we don't display this), location (i.e., filepath), state, 
            # end time (i.e., when file was last used in a pipeline run), and time of creation
            file_entries = []
            while self.node.table_update_queue.empty() is False:
                entry = self.node.table_update_queue.get()
                file_entries.append(entry)
                self.node.table_update_queue.task_done()
            
            # why are the items in the queue being pulled out in reverse (i.e., LIFO) order? 
            file_entries.reverse()
            return file_entries
        
        def format_file_entries(file_entries: List[Dict]) -> List[Dict]:
            # adding on file_display_endpoint to each entry to get the contents of the file when user clicks on row 
            # note: state_change_event_name is used to update the state of the file entry via SSEs
            for file_entry in file_entries:
                file_entry['created_at'] = file_entry['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                file_entry["file_display_endpoint"] = f"{self.get_prefix()}/retrieve_file?file_id={file_entry['id']}"
                file_entry["state_change_event_name"] = f"StateUpdate{file_entry['id']}"
                if file_entry['end_time'] is not None:
                    file_entry['end_time'] = file_entry['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            return file_entries

        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            # clear out queue because we can just get all the rows from the metadata store
            while self.node.table_update_queue.empty() is False:
                entry = self.node.table_update_queue.get()
                self.node.table_update_queue.task_done() 

            file_entries = self.node.metadata_store.get_entries()
            file_entries.reverse()
            file_entries = format_file_entries(file_entries)

            return filesystemstore_home(
                header_bar_endpoint = self.get_header_bar_endpoint(),
                sse_endpoint = self.event_source,
                event_name = self.event_name,
                file_entries = file_entries,
            ) 

        @self.get("/table_update_events", response_class=HTMLResponse)
        async def samples(request: Request):
            async def event_stream():
                while True:
                    try:
                        if await request.is_disconnected():
                            print(f"{self.node.name} SSE disconnected")
                            continue
                        
                        file_entries = get_table_update_events()
                        file_entries = format_file_entries(file_entries)

                        yield "event: TableUpdate\n"
                        yield format_html_for_sse(create_table_rows(file_entries))
                        await asyncio.sleep(0.5)
                    
                    except asyncio.CancelledError:
                        print("browser closed")
                        break 

                    except Exception as e:
                        print(e)
                        break 

            return StreamingResponse(event_stream(), media_type="text/event-stream")

        
        if use_default_file_renderer:
            @self.get("/retrieve_file", response_class=HTMLResponse)
            async def sample(request: Request, file_id: int):
                start = time.time()
                # artifact_path = self.node.get_artifact(file_id)
                # content = self.node.load_artifact(artifact_path)
                # x = filesystemstore_viewer(content, f"Content of {artifact_path}")
                print(time.time() - start)
                return "test file fix"
    