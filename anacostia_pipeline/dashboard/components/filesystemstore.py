from typing import List, Dict

from .node_bar import node_bar_closed



newline = "\n"

def create_table_rows(file_entries: List[Dict[str, str]]):
    return newline.join([
        f'''
        <tr hx-get="{ file_entry["file_display_endpoint"] }" hx-trigger="click" hx-target="#table_container" hx-swap="innerHTML" 
            style="cursor: pointer;">
            <th>{ file_entry["id"] }</th>
            <td>{ file_entry["run_id"] }</td>
            <td>{ file_entry["created_at"] }</td>
            <td>{ file_entry["end_time"] }</td>
            <td>{ file_entry["location"] }</td>
            <td sse-swap="{ file_entry["state_change_event_name"] }" hx-swap="innerHTML">{ file_entry["state"] }</td>
        </tr>
        ''' for file_entry in file_entries
    ])

def filesystemstore_home(header_bar_endpoint: str, sse_endpoint: str, event_name: str, file_entries: List[Dict[str, str]]):
    return f"""
        { node_bar_closed(header_bar_endpoint) }
        <div id="table_container" class="container">
            <table class="table is-bordered is-striped is-hoverable">
                <thead>
                    <tr>
                        <th>file_entry ID</th>
                        <th>Run ID</th>
                        <th>Created At</th>
                        <th>End Time</th>
                        <th>Location</th>
                        <th>State</th>
                    </tr>
                </thead>
                <tbody hx-ext="sse" sse-connect="{sse_endpoint}" sse-swap="{event_name}" hx-swap="afterbegin">
                    { create_table_rows(file_entries) }
                </tbody>
            </table>
        </div>
    """

def filesystemstore_viewer(content: str, box_header: str):
    return f"""
        <div class="block">{ box_header }</div>
        <div class="block box">{ content }</div>
    """
