from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    load_cleaned_posts_from_mongo,
    compute_vader_scores,
    cluster_emotions,
    save_emotion_clusters_to_mongo,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_cleaned_posts_from_mongo,
                inputs=["params:mongo_uri", "params:mongo"],
                outputs="cleaned_posts",
                name="load_cleaned_posts_from_mongo",
            ),
            node(
                func=compute_vader_scores,
                inputs=["cleaned_posts", "params:emotion"],
                outputs="emotion_scored_posts",
                name="compute_vader_scores",
            ),
            node(
                func=cluster_emotions,
                inputs=["emotion_scored_posts", "params:emotion"],
                outputs="clustered_emotion_posts",
                name="cluster_emotions",
            ),
            node(
                func=save_emotion_clusters_to_mongo,
                inputs=[
                    "clustered_emotion_posts",
                    "params:mongo_uri",
                    "params:mongo",
                    "params:emotion",
                ],
                outputs="emotion_save_count",
                name="save_emotion_clusters_to_mongo",
            ),
        ]
    )