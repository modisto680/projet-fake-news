from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    load_raw_posts_from_mongo,
    preprocess_posts,
    save_processed_posts_to_mongo,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_raw_posts_from_mongo,
                inputs=["params:mongo_uri", "params:mongo"],
                outputs="raw_posts",
                name="load_raw_posts_from_mongo",
            ),
            node(
                func=preprocess_posts,
                inputs=["raw_posts", "params:preprocess"],
                outputs="processed_posts",
                name="preprocess_posts",
            ),
            node(
                func=save_processed_posts_to_mongo,
                inputs=["processed_posts", "params:mongo_uri", "params:mongo"],
                outputs="mongo_touched_count",
                name="save_processed_posts_to_mongo",
            ),
        ]
    )
