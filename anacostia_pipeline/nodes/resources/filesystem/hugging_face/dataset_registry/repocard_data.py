from dataclasses import dataclass
import copy
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
import yaml

from anacostia_pipeline.nodes.resources.filesystem.hugging_face.repocard_data import CardData



class DatasetCardData(CardData):
    """Dataset Card Metadata that is used by Hugging Face Hub when included at the top of your README.md

    Args:
        language (`List[str]`, *optional*):
            Language of dataset's data or metadata. It must be an ISO 639-1, 639-2 or
            639-3 code (two/three letters), or a special value like "code", "multilingual".
        license (`Union[str, List[str]]`, *optional*):
            License(s) of this dataset. Example: apache-2.0 or any license from
            https://huggingface.co/docs/hub/repositories-licenses.
        annotations_creators (`Union[str, List[str]]`, *optional*):
            How the annotations for the dataset were created.
            Options are: 'found', 'crowdsourced', 'expert-generated', 'machine-generated', 'no-annotation', 'other'.
        language_creators (`Union[str, List[str]]`, *optional*):
            How the text-based data in the dataset was created.
            Options are: 'found', 'crowdsourced', 'expert-generated', 'machine-generated', 'other'
        multilinguality (`Union[str, List[str]]`, *optional*):
            Whether the dataset is multilingual.
            Options are: 'monolingual', 'multilingual', 'translation', 'other'.
        size_categories (`Union[str, List[str]]`, *optional*):
            The number of examples in the dataset. Options are: 'n<1K', '1K<n<10K', '10K<n<100K',
            '100K<n<1M', '1M<n<10M', '10M<n<100M', '100M<n<1B', '1B<n<10B', '10B<n<100B', '100B<n<1T', 'n>1T', and 'other'.
        source_datasets (`List[str]]`, *optional*):
            Indicates whether the dataset is an original dataset or extended from another existing dataset.
            Options are: 'original' and 'extended'.
        task_categories (`Union[str, List[str]]`, *optional*):
            What categories of task does the dataset support?
        task_ids (`Union[str, List[str]]`, *optional*):
            What specific tasks does the dataset support?
        paperswithcode_id (`str`, *optional*):
            ID of the dataset on PapersWithCode.
        pretty_name (`str`, *optional*):
            A more human-readable name for the dataset. (ex. "Cats vs. Dogs")
        train_eval_index (`Dict`, *optional*):
            A dictionary that describes the necessary spec for doing evaluation on the Hub.
            If not provided, it will be gathered from the 'train-eval-index' key of the kwargs.
        config_names (`Union[str, List[str]]`, *optional*):
            A list of the available dataset configs for the dataset.
    """

    def __init__(
        self,
        *,
        language: Optional[Union[str, List[str]]] = None,
        license: Optional[Union[str, List[str]]] = None,
        annotations_creators: Optional[Union[str, List[str]]] = None,
        language_creators: Optional[Union[str, List[str]]] = None,
        multilinguality: Optional[Union[str, List[str]]] = None,
        size_categories: Optional[Union[str, List[str]]] = None,
        source_datasets: Optional[List[str]] = None,
        task_categories: Optional[Union[str, List[str]]] = None,
        task_ids: Optional[Union[str, List[str]]] = None,
        paperswithcode_id: Optional[str] = None,
        pretty_name: Optional[str] = None,
        train_eval_index: Optional[Dict] = None,
        config_names: Optional[Union[str, List[str]]] = None,
        ignore_metadata_errors: bool = False,
        **kwargs,
    ):
        self.annotations_creators = annotations_creators
        self.language_creators = language_creators
        self.language = language
        self.license = license
        self.multilinguality = multilinguality
        self.size_categories = size_categories
        self.source_datasets = source_datasets
        self.task_categories = task_categories
        self.task_ids = task_ids
        self.paperswithcode_id = paperswithcode_id
        self.pretty_name = pretty_name
        self.config_names = config_names

        # TODO - maybe handle this similarly to EvalResult?
        self.train_eval_index = train_eval_index or kwargs.pop("train-eval-index", None)
        super().__init__(**kwargs)

    def _to_dict(self, data_dict):
        data_dict["train-eval-index"] = data_dict.pop("train_eval_index")