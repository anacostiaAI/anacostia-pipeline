from anacostia_pipeline.nodes.resources.filesystem.hugging_face.dataset_registry.repocard import DatasetCard
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.dataset_registry.repocard_data import DatasetCardData



card_data = DatasetCardData(
    language='en',
    license='mit',
    annotations_creators='crowdsourced',
    task_categories=['text-classification'],
    task_ids=['sentiment-classification', 'text-scoring'],
    multilinguality='monolingual',
    pretty_name='My Text Classification Dataset',
)
card = DatasetCard.from_template(
    card_data,
    pretty_name=card_data.pretty_name,
)
card.save('sample_data_card.md')

"""
# Using a Custom Template
card_data = DatasetCardData(
    language='en',
    license='mit',
)
card = DatasetCard.from_template(
    card_data=card_data,
    template_path='./src/huggingface_hub/templates/datasetcard_template.md',
    custom_template_var='custom value',  # will be replaced in template if it exists
)
"""