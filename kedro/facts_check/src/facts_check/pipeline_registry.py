from facts_check.pipelines.preprocessing.pipeline import create_pipeline as preprocessing_pipeline
from facts_check.pipelines.tfidf_vectorization.pipeline import create_pipeline as tfidf_pipeline

def register_pipelines():
    return {
        "__default__": preprocessing_pipeline() + tfidf_pipeline(),
        "preprocessing": preprocessing_pipeline(),
        "tfidf": tfidf_pipeline(),
    }
