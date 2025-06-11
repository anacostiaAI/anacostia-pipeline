from typing import List, Dict
from anacostia_pipeline.pipelines.fragments import head_template



newline = "\n"

def model_entry_card(model_entry: Dict[str, str], modal_open_endpoint: str = None):
    return f"""
    <div class="model-entry-card">
        <h1>{ model_entry["location"].split("/")[-1] } Metadata</h1>
        <div class="model-entry-row">
            <div class="item"><span class="label">Run ID:</span> { model_entry["run_id"] }</div>
            <div class="item"><span class="label">Model ID:</span> { model_entry["id"] }</div>
            <div class="item"><span class="label">Model State:</span> new</div>
            <div class="item"><span class="label">Created At:</span> { model_entry["created_at"] }</div>
        </div>
        <p><span class="label">Model Path:</span> { model_entry["location"] }</p>

        {
            f'''
            <button class="open-model-card-modal-btn" 
                hx-get="{ modal_open_endpoint }" hx-trigger="click" hx-target="#modal-container" hx-swap="innerHTML">
                Show Model Card
            </button>
            '''
            if modal_open_endpoint is not None else "" 
        }
    </div>
    """

def model_registry_home(update_endpoint: str, model_entries: List[str]):
    return f"""
        {head_template(
            '''
            <!-- Load MathJax config first -->
            <script src="/static/js/src/mathjax-config.js"></script>
            <script type="text/javascript" id="MathJax-script" async src="/static/js/third_party/mathjax.js"></script>

            <!-- CSS for model registry home page -->
            <link rel="stylesheet" href="/static/css/styles/model_registry.css">

            <!-- CSS for markdown modal -->
            <link rel="stylesheet" href="/static/css/styles/markdown.css">
            '''
        )}
        <div id="modal-container"></div>
        <div id="model-entries-container" hx-get="{update_endpoint}" hx-trigger="every 1s" hx-target="this" hx-swap="innerHTML">
            { 
                newline.join(model_entries) 
            }
        </div>
        <script src="/static/js/src/mathjax-reload.js"></script>
    """

def model_card_modal(modal_close_endpoint: str, markdown_html_str: str):
    """
    Renders the markdown file in modal window
    """

    return f"""
    <div class="modal-overlay" hx-get="{modal_close_endpoint}" hx-trigger="click target:.modal-overlay" hx-target="#modal-container">
        <div class="markdown-div page-border">{markdown_html_str}</div>
    </div>
    """