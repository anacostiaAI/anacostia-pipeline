from typing import List


def node_bar_closed(header_bar_endpoint: str):
    return f"""
    <div id="node_header_container">
        <div class="node_header_bar_btn" hx-get="{ header_bar_endpoint }" hx-trigger="click" hx-target="#node_header_container" hx-swap="outerHTML">
            <div class="collapse-btn">▼</div>
        </div>
    </div>
    """

def node_bar_open(node_name: str, status_endpoint: str, work_endpoint: str, header_bar_endpoint: str):
    return f"""
    <div id="node_header_container">
        <div id="node_header_bar">
            <div>Node name: { node_name }</div>
            <!-- Note: add another div to display node type information -->
            <div hx-get="{ status_endpoint }" hx-trigger="load, every 1s" hx-target="this" hx-swap="innerHTML"></div>
            <div hx-get="{ work_endpoint }" hx-trigger="load, every 500ms" hx-target="this" hx-swap="innerHTML"></div>
        </div>
        <div class="node_header_bar_btn" hx-get="{ header_bar_endpoint }" hx-trigger="click" hx-target="#node_header_container" hx-swap="outerHTML">
            <div class="collapse-btn">▲</div>
        </div>
    </div>
    """

def node_page_template(header_bar_endpoint: str, node_content: str):
    return f"""
    <div id="node_header_container" hx-get="{ header_bar_endpoint }" hx-trigger="load" hx-target="this" hx-swap="innerHTML"></div>
    {node_content}
    """

def default_node_page(header_bar_endpoint: str):
    return f"""
    {node_bar_closed(header_bar_endpoint)}
    <div class="container">
        There is no graphical user interface for this node.
    </div>
    """

def work_template(work_list: List[str]):
    newline = "\n"
    return newline.join([f"""<div class="work">{ work }</div>""" for work in work_list])
