from typing import List, Dict



def node_bar_invisible(node_model: Dict[str, str]) -> str:
    return f"""
    <div id="{node_model["id"]}_header_div" 
        hx-get="{node_model["header_bar_endpoint"]}" 
        hx-trigger="click from:#{node_model["id"]}, click from:#{node_model["id"]}_tab" 
        hx-swap-oob="true">
    </div>
    """

def node_bar_closed(node_model: Dict[str, str], open_div_endpoint: str) -> str:
    return f"""
    <div id="{node_model["id"]}_header_div" 
        hx-get="{open_div_endpoint}" 
        hx-trigger="click from:#{node_model["id"]}, click from:#{node_model["id"]}_tab, click from:.node_header_bar_btn" 
        hx-swap-oob="true">
        <div class="node_header_bar_btn">
            <div class="collapse-btn">▼</div>
        </div>
    </div>
    """

def node_bar_open(node_model: Dict[str, str], close_div_endpoint: str) -> str:
    return f"""
    <div id="{node_model["id"]}_header_div" 
        hx-get="{close_div_endpoint}" 
        hx-trigger="click from:#{node_model["id"]}, click from:#{node_model["id"]}_tab, click from:.node_header_bar_btn" 
        hx-swap-oob="true">
        <div id="node_header_bar">
            <div>Node name: { node_model['id'] }</div>
            <!-- Note: add another div to display node type information -->
            <div hx-get="{ node_model['status_endpoint'] }" hx-trigger="load, every 1s" hx-target="this" hx-swap="innerHTML"></div>
            <div hx-get="{ node_model['work_endpoint'] }" hx-trigger="load, every 500ms" hx-target="this" hx-swap="innerHTML"></div>
        </div>
        <div class="node_header_bar_btn">
            <div class="collapse-btn">▲</div>
        </div>
    </div>
    """

def default_node_page(header_bar_endpoint: str) -> str:
    return f"""
    {node_bar_closed(header_bar_endpoint)}
    <div class="container">
        There is no graphical user interface for this node.
    </div>
    """

def work_template(work_list: List[str]) -> str:
    newline = "\n"
    return newline.join([f"""<div class="work">{ work }</div>""" for work in work_list])
