from typing import List, Dict
from anacostia_pipeline.pipelines.fragments import head_template



newline = "\n"


def data_entry_card(model_entry: Dict[str, str], modal_open_endpoint: str = None):
    return f"""
    """

def dataset_registry_home(update_endpoint: str, dataset_entries: List[str] = None):
    return f"""
        {head_template(
            '''
            '''
        )}
        <div id="modal-container"></div>
        <div id="model-entries-container" hx-get="{update_endpoint}" hx-trigger="every 1s" hx-target="this" hx-swap="innerHTML">
            {
                newline.join(
                    [
                        f'<div>{entry}</div>' for entry in dataset_entries
                    ]
                )
            }
        </div>
    """

def data_card_modal(modal_close_endpoint: str, markdown_html_str: str):
    """
    Renders the markdown file in modal window
    """

    return f"""
    <div class="modal-overlay" hx-get="{modal_close_endpoint}" hx-trigger="click target:.modal-overlay" hx-target="#modal-container">
        <div class="markdown-div page-border">{markdown_html_str}</div>
    </div>
    """