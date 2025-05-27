from typing import Any, Dict, Literal, Optional, Type, Union
from pathlib import Path

from anacostia_pipeline.nodes.resources.filesystem.hugging_face.repocard import RepoCard
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.dataset_registry.repocard_data import DatasetCardData

TEMPLATE_DATASETCARD_PATH = Path(__file__).parent / "templates" / "datasetcard_template.md"


class DatasetCard(RepoCard):
    card_data_class = DatasetCardData
    default_template_path = TEMPLATE_DATASETCARD_PATH
    repo_type = "dataset"

    @classmethod
    def from_template(  # type: ignore # violates Liskov property but easier to use
        cls,
        card_data: DatasetCardData,
        template_path: Optional[str] = None,
        template_str: Optional[str] = None,
        **template_kwargs,
    ):
        """Initialize a DatasetCard from a template. By default, it uses the default template, which can be found here:
        https://github.com/huggingface/huggingface_hub/blob/main/src/huggingface_hub/templates/datasetcard_template.md

        Templates are Jinja2 templates that can be customized by passing keyword arguments.

        Args:
            card_data (`huggingface_hub.DatasetCardData`):
                A huggingface_hub.DatasetCardData instance containing the metadata you want to include in the YAML
                header of the dataset card on the Hugging Face Hub.
            template_path (`str`, *optional*):
                A path to a markdown file with optional Jinja template variables that can be filled
                in with `template_kwargs`. Defaults to the default template.

        Returns:
            [`huggingface_hub.DatasetCard`]: A DatasetCard instance with the specified card data and content from the
            template.

        Example:
            ```python
            >>> from anacostia_pipeline.nodes.resources.filesystem.model_registry.repocard import DatasetCard
            >>> from anacostia_pipeline.nodes.resources.filesystem.model_registry.repocard_data import DatasetCardData

            >>> # Using the Default Template
            >>> card_data = DatasetCardData(
            ...     language='en',
            ...     license='mit',
            ...     annotations_creators='crowdsourced',
            ...     task_categories=['text-classification'],
            ...     task_ids=['sentiment-classification', 'text-scoring'],
            ...     multilinguality='monolingual',
            ...     pretty_name='My Text Classification Dataset',
            ... )
            >>> card = DatasetCard.from_template(
            ...     card_data,
            ...     pretty_name=card_data.pretty_name,
            ... )

            >>> # Using a Custom Template
            >>> card_data = DatasetCardData(
            ...     language='en',
            ...     license='mit',
            ... )
            >>> card = DatasetCard.from_template(
            ...     card_data=card_data,
            ...     template_path='./src/huggingface_hub/templates/datasetcard_template.md',
            ...     custom_template_var='custom value',  # will be replaced in template if it exists
            ... )

            ```
        """
        return super().from_template(card_data, template_path, template_str, **template_kwargs)
