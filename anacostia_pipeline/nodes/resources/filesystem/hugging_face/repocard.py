import os
import re
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Type, Union
import yaml

from anacostia_pipeline.nodes.resources.filesystem.hugging_face.repocard_data import CardData

TEMPLATE_MODELCARD_PATH = Path(__file__).parent / "model_registry" / "templates" / "modelcard_template.md"

# exact same regex as in the Hub server. Please keep in sync.
# See https://github.com/huggingface/moon-landing/blob/main/server/lib/ViewMarkdown.ts#L18
REGEX_YAML_BLOCK = re.compile(r"^(\s*---[\r\n]+)([\S\s]*?)([\r\n]+---(\r\n|\n|$))")



class RepoCard:
    card_data_class = CardData
    default_template_path = TEMPLATE_MODELCARD_PATH
    repo_type = "model"

    def __init__(self, content: str, ignore_metadata_errors: bool = False):
        """Initialize a huggingface-style RepoCard from string content. The content should be a
        Markdown file with a YAML block at the beginning and a Markdown body.

        Args:
            content (`str`): The content of the Markdown file.

        Example:
            ```python
            >>> from anacostia_pipeline.nodes.resources.filesystem.model_registry.repocard import RepoCard
            >>> text = '''
            ... ---
            ... language: en
            ... license: mit
            ... ---
            ...
            ... # My repo
            ... '''
            >>> card = RepoCard(text)
            >>> card.data.to_dict()
            {'language': 'en', 'license': 'mit'}
            >>> card.text
            '\\n# My repo\\n'

            ```
        <Tip>
        Raises the following error:

            - [`ValueError`](https://docs.python.org/3/library/exceptions.html#ValueError)
              when the content of the repo card metadata is not a dictionary.

        </Tip>
        """

        # Set the content of the RepoCard, as well as underlying .data and .text attributes.
        # See the `content` property setter for more details.
        self.ignore_metadata_errors = ignore_metadata_errors
        self.content = content

    @property
    def content(self):
        """The content of the RepoCard, including the YAML block and the Markdown body."""
        line_break = _detect_line_ending(self._content) or "\n"
        return f"---{line_break}{self.data.to_yaml(line_break=line_break, original_order=self._original_order)}{line_break}---{line_break}{self.text}"

    @content.setter
    def content(self, content: str):
        """Set the content of the RepoCard."""
        self._content = content

        match = REGEX_YAML_BLOCK.search(content)
        if match:
            # Metadata found in the YAML block
            yaml_block = match.group(2)
            self.text = content[match.end() :]
            data_dict = yaml.safe_load(yaml_block)

            if data_dict is None:
                data_dict = {}

            # The YAML block's data should be a dictionary
            if not isinstance(data_dict, dict):
                raise ValueError("repo card metadata block should be a dict")
        else:
            # Model card without metadata... create empty metadata
            print("Repo card metadata block was not found. Setting CardData to empty.")
            data_dict = {}
            self.text = content

        self.data = self.card_data_class(**data_dict, ignore_metadata_errors=self.ignore_metadata_errors)
        self._original_order = list(data_dict.keys())

    def __str__(self):
        return self.content

    def save(self, filepath: Union[Path, str]):
        r"""Save a RepoCard to a file.

        Args:
            filepath (`Union[Path, str]`): Filepath to the markdown file to save.

        Example:
            ```python
            >>> from anacostia_pipeline.nodes.resources.filesystem.model_registry.repocard import RepoCard
            >>> card = RepoCard("---\nlanguage: en\n---\n# This is a test repo card")
            >>> card.save("/tmp/test.md")

            ```
        """
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        # Preserve newlines as in the existing file.
        with open(filepath, mode="w", newline="", encoding="utf-8") as f:
            f.write(str(self))

    @classmethod
    def load(
        cls,
        repo_id_or_path: Union[str, Path],
        repo_type: Optional[str] = None,
        token: Optional[str] = None,
        ignore_metadata_errors: bool = False,
    ):
        """Initialize a RepoCard from a local filepath.

        Args:
            repo_id_or_path (`Union[str, Path]`):
                The repo ID associated with a Hugging Face Hub repo or a local filepath.
            repo_type (`str`, *optional*):
                The type of Hugging Face repo to push to. Defaults to None, which will use use "model". Other options
                are "dataset" and "space". Not used when loading from a local filepath. If this is called from a child
                class, the default value will be the child class's `repo_type`.
            token (`str`, *optional*):
                Authentication token, obtained with `huggingface_hub.HfApi.login` method. Will default to the stored token.
            ignore_metadata_errors (`str`):
                If True, errors while parsing the metadata section will be ignored. Some information might be lost during
                the process. Use it at your own risk.

        Returns:
            [`huggingface_hub.repocard.RepoCard`]: The RepoCard (or subclass) initialized from the repo's
                README.md file or filepath.

        Example:
            ```python
            >>> from anacostia_pipeline.nodes.resources.filesystem.model_registry.repocard import RepoCard
            >>> card = RepoCard.load("nateraw/food")
            >>> assert card.data.tags == ["generated_from_trainer", "image-classification", "pytorch"]

            ```
        """

        if Path(repo_id_or_path).is_file():
            card_path = Path(repo_id_or_path)
        else:
            raise ValueError(f"Cannot load RepoCard: path not found on disk ({repo_id_or_path}).")

        # Preserve newlines in the existing file.
        with card_path.open(mode="r", newline="", encoding="utf-8") as f:
            return cls(f.read(), ignore_metadata_errors=ignore_metadata_errors)


    @classmethod
    def from_template(
        cls,
        card_data: CardData,
        template_path: Optional[str] = None,
        template_str: Optional[str] = None,
        **template_kwargs,
    ):
        """Initialize a RepoCard from a template. By default, it uses the default template.

        Templates are Jinja2 templates that can be customized by passing keyword arguments.

        Args:
            card_data (`huggingface_hub.CardData`):
                A huggingface_hub.CardData instance containing the metadata you want to include in the YAML
                header of the repo card on the Hugging Face Hub.
            template_path (`str`, *optional*):
                A path to a markdown file with optional Jinja template variables that can be filled
                in with `template_kwargs`. Defaults to the default template.

        Returns:
            [`huggingface_hub.repocard.RepoCard`]: A RepoCard instance with the specified card data and content from the
            template.
        """
        try:
            import jinja2
        except ImportError:
            raise ImportError("Using RepoCard.from_template requires Jinja2 to be installed. Please install it with `pip install Jinja2`.")

        kwargs = card_data.to_dict().copy()
        kwargs.update(template_kwargs)  # Template_kwargs have priority

        if template_path is not None:
            template_str = Path(template_path).read_text()
        if template_str is None:
            template_str = Path(cls.default_template_path).read_text()
        template = jinja2.Template(template_str)
        content = template.render(card_data=card_data.to_yaml(), **kwargs)
        return cls(content)


def _detect_line_ending(content: str) -> Literal["\r", "\n", "\r\n", None]:  # noqa: F722
    """Detect the line ending of a string. Used by RepoCard to avoid making huge diff on newlines.

    Uses same implementation as in Hub server, keep it in sync.

    Returns:
        str: The detected line ending of the string.
    """
    cr = content.count("\r")
    lf = content.count("\n")
    crlf = content.count("\r\n")
    if cr + lf == 0:
        return None
    if crlf == cr and crlf == lf:
        return "\r\n"
    if cr > lf:
        return "\r"
    else:
        return "\n"