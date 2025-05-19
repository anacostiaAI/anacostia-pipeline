import os
import re
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Type, Union
import yaml

from anacostia_pipeline.nodes.resources.filesystem.hugging_face.repocard import RepoCard
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard_data import ModelCardData


TEMPLATE_MODELCARD_PATH = Path(__file__).parent / "templates" / "modelcard_template.md"

# exact same regex as in the Hub server. Please keep in sync.
# See https://github.com/huggingface/moon-landing/blob/main/server/lib/ViewMarkdown.ts#L18
REGEX_YAML_BLOCK = re.compile(r"^(\s*---[\r\n]+)([\S\s]*?)([\r\n]+---(\r\n|\n|$))")




class ModelCard(RepoCard):
    card_data_class = ModelCardData
    default_template_path = TEMPLATE_MODELCARD_PATH
    repo_type = "model"

    @classmethod
    def from_template(  # type: ignore # violates Liskov property but easier to use
        cls,
        card_data: ModelCardData,
        template_path: Optional[str] = None,
        template_str: Optional[str] = None,
        **template_kwargs,
    ):
        """Initialize a ModelCard from a template. By default, it uses the default template, which can be found here:
        https://github.com/huggingface/huggingface_hub/blob/main/src/huggingface_hub/templates/modelcard_template.md

        Templates are Jinja2 templates that can be customized by passing keyword arguments.

        Args:
            card_data (`huggingface_hub.ModelCardData`):
                A huggingface_hub.ModelCardData instance containing the metadata you want to include in the YAML
                header of the model card on the Hugging Face Hub.
            template_path (`str`, *optional*):
                A path to a markdown file with optional Jinja template variables that can be filled
                in with `template_kwargs`. Defaults to the default template.

        Returns:
            [`huggingface_hub.ModelCard`]: A ModelCard instance with the specified card data and content from the
            template.

        Example:
            ```python
            >>> from anacostia_pipeline.nodes.resources.filesystem.model_registry.repocard import ModelCard
            >>> from anacostia_pipeline.nodes.resources.filesystem.model_registry.repocard_data import ModelCardData, EvalResult

            >>> # Using the Default Template
            >>> card_data = ModelCardData(
            ...     language='en',
            ...     license='mit',
            ...     library_name='timm',
            ...     tags=['image-classification', 'resnet'],
            ...     datasets=['beans'],
            ...     metrics=['accuracy'],
            ... )
            >>> card = ModelCard.from_template(
            ...     card_data,
            ...     model_description='This model does x + y...'
            ... )

            >>> # Including Evaluation Results
            >>> card_data = ModelCardData(
            ...     language='en',
            ...     tags=['image-classification', 'resnet'],
            ...     eval_results=[
            ...         EvalResult(
            ...             task_type='image-classification',
            ...             dataset_type='beans',
            ...             dataset_name='Beans',
            ...             metric_type='accuracy',
            ...             metric_value=0.9,
            ...         ),
            ...     ],
            ...     model_name='my-cool-model',
            ... )
            >>> card = ModelCard.from_template(card_data)

            >>> # Using a Custom Template
            >>> card_data = ModelCardData(
            ...     language='en',
            ...     tags=['image-classification', 'resnet']
            ... )
            >>> card = ModelCard.from_template(
            ...     card_data=card_data,
            ...     template_path='./src/huggingface_hub/templates/modelcard_template.md',
            ...     custom_template_var='custom value',  # will be replaced in template if it exists
            ... )

            ```
        """
        return super().from_template(card_data, template_path, template_str, **template_kwargs)
