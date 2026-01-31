from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    load_posts_processed_from_mongo,
    fit_transform_tfidf,
    save_tfidf_artefacts,
    save_posts_tfidf_to_mongo,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_posts_processed_from_mongo,
                inputs=["params:mongo_uri", "params:mongo", "params:tfidf"],
                outputs="posts_processed_df",
                name="load_posts_processed_from_mongo",
            ),
            node(
                func=fit_transform_tfidf,
                inputs=["posts_processed_df", "params:tfidf"],
                outputs=["posts_tfidf_pkl", "tfidf_artefacts"],
                name="fit_transform_tfidf",
            ),
            node(
                func=save_tfidf_artefacts,
                inputs=["tfidf_artefacts", "params:tfidf"],
                outputs="tfidf_saved_paths",
                name="save_tfidf_artefacts",
            ),
            node(
                func=save_posts_tfidf_to_mongo,
                inputs=["posts_processed_df", "params:mongo_uri", "params:mongo", "params:tfidf"],
                outputs="mongo_tfidf_touched_count",
                name="save_posts_tfidf_to_mongo",
            ),
        ]
    )
