from typing import List, Dict

from .node_bar import node_bar_closed



newline = "\n"

def filesystemstore_table(file_entries_endpoint: str, file_entries: List[Dict[str, str]]):
    return f"""
        <table class="table is-bordered is-striped is-hoverable"
            hx-get="{ file_entries_endpoint }" hx-trigger="every 1s" hx-swap="outerHTML" hx-target="this" hx-sync="tr:abort">
            <!--
            hx-sync="tr:abort" is used to ensure the file_entries_endpoint doesn't trigger when one of the rows is clicked on.
            I believe this is due to the table considering the row to be part of the table, so it will trigger the file_entries_endpoint
            when the row is clicked on.
            -->
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
            <tbody>
                {
                    newline.join([
                        f'''
                        <tr hx-get="{ file_entry["file_display_endpoint"] }" hx-trigger="click" hx-target="#table_container" hx-swap="innerHTML" 
                            style="cursor: pointer;">
                            <th>{ file_entry["id"] }</th>
                            <td>{ file_entry["run_id"] }</td>
                            <td>{ file_entry["created_at"] }</td>
                            <td>{ file_entry["end_time"] }</td>
                            <td>{ file_entry["location"] }</td>
                            <td>{ file_entry["state"] }</td>
                        </tr>
                        ''' for file_entry in file_entries
                    ])
                }
            </tbody>
        </table>
    """

def filesystemstore_home(header_bar_endpoint: str, file_entries_endpoint: str, file_entries: List[Dict[str, str]]):
    return f"""
        { node_bar_closed(header_bar_endpoint) }
        <div id="table_container" class="container">
            { filesystemstore_table(file_entries_endpoint, file_entries) }
        </div>
    """

def filesystemstore_viewer(content: str, box_header: str):
    return f"""
        <div class="block">{ box_header }</div>
        <div class="block box">{ content }</div>
    """
