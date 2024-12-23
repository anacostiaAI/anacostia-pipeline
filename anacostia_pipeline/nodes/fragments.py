from typing import Set


def default_node_page() -> str:
    return f"""
    <div class="container">
        There is no graphical user interface for this node.
    </div>
    """

def work_template(work_set: Set[str]) -> str:
    newline = "\n"
    return newline.join([f"""<div class="work">{ work }</div>""" for work in work_set])