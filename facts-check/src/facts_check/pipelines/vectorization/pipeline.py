from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    load_processed_posts_from_mongo,
    compute_embeddings,
    cluster_embeddings,
    save_clustered_posts_to_mongo,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_processed_posts_from_mongo,
                inputs=["params:mongo_uri", "params:mongo", "params:vectorization"],
                outputs="processed_posts_for_vectorization",
                name="load_processed_posts_from_mongo",
            ),
            node(
                func=compute_embeddings,
                inputs=["processed_posts_for_vectorization", "params:vectorization"],
                outputs="embedding_payload",
                name="compute_embeddings",
            ),
            node(
                func=cluster_embeddings,
                inputs=["embedding_payload", "params:clustering"],
                outputs="clustered_posts",
                name="cluster_embeddings",
            ),
            node(
                func=save_clustered_posts_to_mongo,
                inputs=["clustered_posts", "params:mongo_uri", "params:mongo", "params:vectorization"],
                outputs="mongo_clustered_touched_count",
                name="save_clustered_posts_to_mongo",
            ),
        ]
    )
