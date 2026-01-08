from facts_check.pipelines.preprocessing.pipeline import create_pipeline as preprocessing_pipeline
from facts_check.pipelines.vectorization.pipeline import create_pipeline as vectorization_pipeline

# ancien register à effacer si tout OK
# def register_pipelines():
#     return {
#         "__default__": preprocessing_pipeline(),
#         "preprocessing": preprocessing_pipeline(),
#     }

def register_pipelines():
    return {
        "__default__": preprocessing_pipeline() + vectorization_pipeline(),
        "preprocessing": preprocessing_pipeline(),
        "vectorization": vectorization_pipeline(),
    }