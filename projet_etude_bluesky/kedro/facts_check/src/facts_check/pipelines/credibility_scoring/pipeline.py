from kedro.pipeline import Pipeline, node

from .nodes import (
    load_cleaned_posts_for_scoring,
    score_posts,
    save_scored_posts_to_mongo,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=load_cleaned_posts_for_scoring,
                inputs=[
                    "params:mongo_uri",
                    "params:mongo",
                    "params:credibility",
                ],
                outputs="cleaned_posts_for_scoring",
                name="load_cleaned_posts_for_scoring_node",
            ),
            node(
                func=score_posts,
                inputs=[
                    "cleaned_posts_for_scoring",
                    "fake_news_model",
                    "params:credibility",
                ],
                outputs="scored_posts_df",
                name="score_posts_node",
            ),
            node(
                func=save_scored_posts_to_mongo,
                inputs=[
                    "scored_posts_df",
                    "params:mongo_uri",
                    "params:mongo",
                    "params:credibility",
                ],
                outputs="n_scored_posts",
                name="save_scored_posts_node",
            ),
        ]
    )
