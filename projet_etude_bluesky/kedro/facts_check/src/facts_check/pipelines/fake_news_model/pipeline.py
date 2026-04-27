from kedro.pipeline import Pipeline, node
from .nodes import prepare_training_data, train_model


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=prepare_training_data,
                inputs=["fake_news_raw", "true_news_raw"],
                outputs="training_data",
                name="prepare_training_data_node",
            ),
            node(
                func=train_model,
                inputs="training_data",
                outputs=["fake_news_model", "fake_news_metrics"],
                name="train_model_node",
            ),
        ]
    )