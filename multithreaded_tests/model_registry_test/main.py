from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard import ModelCard
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard_data import ModelCardData, EvalResult


"""
card_data = ModelCardData(
    language='en',
    license='mit',
    library_name='timm',
    tags=['image-classification', 'resnet'],
    datasets=['beans'],
    metrics=['accuracy'],
)

card = ModelCard.from_template(
    card_data,
    model_description='This model does x + y...'
)

"""

card_data = ModelCardData(
language='en',
tags=['image-classification', 'resnet'],
eval_results=[
    EvalResult(
        task_type='image-classification',
        dataset_type='beans',
        dataset_name='Beans',
        metric_type='accuracy',
        metric_value=0.9,
    ),
],
model_name='my-cool-model')

card = ModelCard.from_template(
    card_data,
    model_description='This model does x + y...'
)
card.save('sample_model_card.md')