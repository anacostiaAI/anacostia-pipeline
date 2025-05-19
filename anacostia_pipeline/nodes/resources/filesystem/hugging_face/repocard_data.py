from anacostia_pipeline.nodes.resources.filesystem.hugging_face.utils import yaml_dump
from dataclasses import dataclass
import copy
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union



@dataclass
class EvalResult:
    """
    Flattened representation of individual evaluation results found in model-index of Model Cards.

    For more information on the model-index spec, see https://github.com/huggingface/hub-docs/blob/main/modelcard.md?plain=1.

    Args:
        task_type (`str`):
            The task identifier. Example: "image-classification".
        dataset_type (`str`):
            The dataset identifier. Example: "common_voice". Use dataset id from https://hf.co/datasets.
        dataset_name (`str`):
            A pretty name for the dataset. Example: "Common Voice (French)".
        metric_type (`str`):
            The metric identifier. Example: "wer". Use metric id from https://hf.co/metrics.
        metric_value (`Any`):
            The metric value. Example: 0.9 or "20.0 ± 1.2".
        task_name (`str`, *optional*):
            A pretty name for the task. Example: "Speech Recognition".
        dataset_config (`str`, *optional*):
            The name of the dataset configuration used in `load_dataset()`.
            Example: fr in `load_dataset("common_voice", "fr")`. See the `datasets` docs for more info:
            https://hf.co/docs/datasets/package_reference/loading_methods#datasets.load_dataset.name
        dataset_split (`str`, *optional*):
            The split used in `load_dataset()`. Example: "test".
        dataset_revision (`str`, *optional*):
            The revision (AKA Git Sha) of the dataset used in `load_dataset()`.
            Example: 5503434ddd753f426f4b38109466949a1217c2bb
        dataset_args (`Dict[str, Any]`, *optional*):
            The arguments passed during `Metric.compute()`. Example for `bleu`: `{"max_order": 4}`
        metric_name (`str`, *optional*):
            A pretty name for the metric. Example: "Test WER".
        metric_config (`str`, *optional*):
            The name of the metric configuration used in `load_metric()`.
            Example: bleurt-large-512 in `load_metric("bleurt", "bleurt-large-512")`.
            See the `datasets` docs for more info: https://huggingface.co/docs/datasets/v2.1.0/en/loading#load-configurations
        metric_args (`Dict[str, Any]`, *optional*):
            The arguments passed during `Metric.compute()`. Example for `bleu`: max_order: 4
        verified (`bool`, *optional*):
            Indicates whether the metrics originate from Hugging Face's [evaluation service](https://huggingface.co/spaces/autoevaluate/model-evaluator) or not. Automatically computed by Hugging Face, do not set.
        verify_token (`str`, *optional*):
            A JSON Web Token that is used to verify whether the metrics originate from Hugging Face's [evaluation service](https://huggingface.co/spaces/autoevaluate/model-evaluator) or not.
        source_name (`str`, *optional*):
            The name of the source of the evaluation result. Example: "Open LLM Leaderboard".
        source_url (`str`, *optional*):
            The URL of the source of the evaluation result. Example: "https://huggingface.co/spaces/open-llm-leaderboard/open_llm_leaderboard".
    """

    # Required

    # The task identifier
    # Example: automatic-speech-recognition
    task_type: str

    # The dataset identifier
    # Example: common_voice. Use dataset id from https://hf.co/datasets
    dataset_type: str

    # A pretty name for the dataset.
    # Example: Common Voice (French)
    dataset_name: str

    # The metric identifier
    # Example: wer. Use metric id from https://hf.co/metrics
    metric_type: str

    # Value of the metric.
    # Example: 20.0 or "20.0 ± 1.2"
    metric_value: Any

    # Optional

    # A pretty name for the task.
    # Example: Speech Recognition
    task_name: Optional[str] = None

    # The name of the dataset configuration used in `load_dataset()`.
    # Example: fr in `load_dataset("common_voice", "fr")`.
    # See the `datasets` docs for more info:
    # https://huggingface.co/docs/datasets/package_reference/loading_methods#datasets.load_dataset.name
    dataset_config: Optional[str] = None

    # The split used in `load_dataset()`.
    # Example: test
    dataset_split: Optional[str] = None

    # The revision (AKA Git Sha) of the dataset used in `load_dataset()`.
    # Example: 5503434ddd753f426f4b38109466949a1217c2bb
    dataset_revision: Optional[str] = None

    # The arguments passed during `Metric.compute()`.
    # Example for `bleu`: max_order: 4
    dataset_args: Optional[Dict[str, Any]] = None

    # A pretty name for the metric.
    # Example: Test WER
    metric_name: Optional[str] = None

    # The name of the metric configuration used in `load_metric()`.
    # Example: bleurt-large-512 in `load_metric("bleurt", "bleurt-large-512")`.
    # See the `datasets` docs for more info: https://huggingface.co/docs/datasets/v2.1.0/en/loading#load-configurations
    metric_config: Optional[str] = None

    # The arguments passed during `Metric.compute()`.
    # Example for `bleu`: max_order: 4
    metric_args: Optional[Dict[str, Any]] = None

    # Indicates whether the metrics originate from Hugging Face's [evaluation service](https://huggingface.co/spaces/autoevaluate/model-evaluator) or not. Automatically computed by Hugging Face, do not set.
    verified: Optional[bool] = None

    # A JSON Web Token that is used to verify whether the metrics originate from Hugging Face's [evaluation service](https://huggingface.co/spaces/autoevaluate/model-evaluator) or not.
    verify_token: Optional[str] = None

    # The name of the source of the evaluation result.
    # Example: Open LLM Leaderboard
    source_name: Optional[str] = None

    # The URL of the source of the evaluation result.
    # Example: https://huggingface.co/spaces/open-llm-leaderboard/open_llm_leaderboard
    source_url: Optional[str] = None

    @property
    def unique_identifier(self) -> tuple:
        """Returns a tuple that uniquely identifies this evaluation."""
        return (
            self.task_type,
            self.dataset_type,
            self.dataset_config,
            self.dataset_split,
            self.dataset_revision,
        )

    def is_equal_except_value(self, other: "EvalResult") -> bool:
        """
        Return True if `self` and `other` describe exactly the same metric but with a
        different value.
        """
        for key, _ in self.__dict__.items():
            if key == "metric_value":
                continue
            # For metrics computed by Hugging Face's evaluation service, `verify_token` is derived from `metric_value`,
            # so we exclude it here in the comparison.
            if key != "verify_token" and getattr(self, key) != getattr(other, key):
                return False
        return True

    def __post_init__(self) -> None:
        if self.source_name is not None and self.source_url is None:
            raise ValueError("If `source_name` is provided, `source_url` must also be provided.")



@dataclass
class CardData:
    """Structure containing metadata from a RepoCard.

    [`CardData`] is the parent class of [`ModelCardData`] and [`DatasetCardData`].

    Metadata can be exported as a dictionary or YAML. Export can be customized to alter the representation of the data
    (example: flatten evaluation results). `CardData` behaves as a dictionary (can get, pop, set values) but do not
    inherit from `dict` to allow this export step.
    """

    def __init__(self, ignore_metadata_errors: bool = False, **kwargs):
        self.__dict__.update(kwargs)

    def to_dict(self):
        """Converts CardData to a dict.

        Returns:
            `dict`: CardData represented as a dictionary ready to be dumped to a YAML
            block for inclusion in a README.md file.
        """

        data_dict = copy.deepcopy(self.__dict__)
        self._to_dict(data_dict)
        return {key: value for key, value in data_dict.items() if value is not None}

    def _to_dict(self, data_dict):
        """Use this method in child classes to alter the dict representation of the data. Alter the dict in-place.

        Args:
            data_dict (`dict`): The raw dict representation of the card data.
        """
        pass

    def to_yaml(self, line_break=None, original_order: Optional[List[str]] = None) -> str:
        """Dumps CardData to a YAML block for inclusion in a README.md file.

        Args:
            line_break (str, *optional*):
                The line break to use when dumping to yaml.

        Returns:
            `str`: CardData represented as a YAML block.
        """
        if original_order:
            self.__dict__ = {
                k: self.__dict__[k]
                for k in original_order + list(set(self.__dict__.keys()) - set(original_order))
                if k in self.__dict__
            }
        return yaml_dump(self.to_dict(), sort_keys=False, line_break=line_break).strip()

    def __repr__(self):
        return repr(self.__dict__)

    def __str__(self):
        return self.to_yaml()

    def get(self, key: str, default: Any = None) -> Any:
        """Get value for a given metadata key."""
        value = self.__dict__.get(key)
        return default if value is None else value

    def pop(self, key: str, default: Any = None) -> Any:
        """Pop value for a given metadata key."""
        return self.__dict__.pop(key, default)

    def __getitem__(self, key: str) -> Any:
        """Get value for a given metadata key."""
        return self.__dict__[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Set value for a given metadata key."""
        self.__dict__[key] = value

    def __contains__(self, key: str) -> bool:
        """Check if a given metadata key is set."""
        return key in self.__dict__

    def __len__(self) -> int:
        """Return the number of metadata keys set."""
        return len(self.__dict__)


def _validate_eval_results(
    eval_results: Optional[Union[EvalResult, List[EvalResult]]],
    model_name: Optional[str],
) -> List[EvalResult]:
    if eval_results is None:
        return []
    if isinstance(eval_results, EvalResult):
        eval_results = [eval_results]
    if not isinstance(eval_results, list) or not all(isinstance(r, EvalResult) for r in eval_results):
        raise ValueError(
            f"`eval_results` should be of type `EvalResult` or a list of `EvalResult`, got {type(eval_results)}."
        )
    if model_name is None:
        raise ValueError("Passing `eval_results` requires `model_name` to be set.")
    return eval_results