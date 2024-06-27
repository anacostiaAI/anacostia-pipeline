def format_html_for_sse(html_content: str) -> str:
    """
    Function for converting html into a message compatible with Server Sent Events.
    """

    # Split the HTML content into lines
    lines = html_content.split('\n')

    # Prefix each line with 'data: ' and join them back into a single string with newlines
    formatted_content = "\n".join(f"data: {line}" for line in lines if line.strip()) + "\n\n"

    return formatted_content