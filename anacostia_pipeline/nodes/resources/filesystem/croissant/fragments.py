from typing import List, Dict
from anacostia_pipeline.pipelines.fragments import head_template



newline = "\n"


def data_entry_card(data_card_entry: Dict[str, str], modal_open_endpoint: str = None):
    return f"""
    <div class="model-entry-card">
        <h1>{ data_card_entry["location"] }</h1>
        <div class="model-entry-row">
        </div>

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

def dataset_registry_home(update_endpoint: str, dataset_entries: List[str] = None):
    return f"""
        {head_template(
            '''
            <!-- CSS for model registry home page -->
            <link rel="stylesheet" href="/static/css/styles/model_registry.css">

            <!-- CSS for markdown modal -->
            <link rel="stylesheet" href="/static/css/styles/markdown.css">
            '''
        )}
        <div id="modal-container"></div>
        <div id="model-entries-container" hx-get="{update_endpoint}" hx-trigger="every 1s" hx-target="this" hx-swap="innerHTML">
            { newline.join(dataset_entries) }
        </div>
    """

def data_card_modal(modal_close_endpoint: str, modal_html_str: str):
    """
    Renders the markdown file in modal window
    """

    return f"""
    <div class="modal-overlay" hx-get="{modal_close_endpoint}" hx-trigger="click target:.modal-overlay" hx-target="#modal-container">
        <div class="markdown-div page-border">
            <pre>{modal_html_str}</pre>
        </div>
    </div>
    """