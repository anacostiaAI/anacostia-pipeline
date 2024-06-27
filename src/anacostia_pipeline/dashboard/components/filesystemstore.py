from typing import List, Dict



newline = "\n"

def table_row(file_entry: Dict[str, str]):
    return f"""
        <tr hx-get="{ file_entry["file_display_endpoint"] }" hx-trigger="click" hx-target="#table_container" hx-swap="innerHTML" 
            style="cursor: pointer;">
            <th>{ file_entry["id"] }</th>
            <td>{ file_entry["run_id"] }</td>
            <td>{ file_entry["created_at"] }</td>
            <td>{ file_entry["end_time"] }</td>
            <td>{ file_entry["location"] }</td>
            <td sse-swap="{ file_entry["state_change_event_name"] }" hx-target="closest tr" hx-swap="outerHTML">{ file_entry["state"] }</td>
        </tr>
    """

def create_table_rows(file_entries: List[Dict[str, str]]):
    return newline.join([
        table_row(file_entry) for file_entry in file_entries
    ])

def filesystemstore_home(sse_endpoint: str, event_name: str, file_entries: List[Dict[str, str]]):
    return f"""
        <link rel="stylesheet" type="text/css" href="/static/css/styles/filesystemstore.css">
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
