from facts_check.pipelines.preprocessing.pipeline import create_pipeline as preprocessing_pipeline
from facts_check.pipelines.tfidf_vectorization.pipeline import create_pipeline as tfidf_pipeline
from facts_check.pipelines.fake_news_model import pipeline as fake_news_model
from facts_check.pipelines.emotion_analysis import pipeline as emotion_analysis
from facts_check.pipelines.credibility_scoring import pipeline as credibility_scoring

def register_pipelines():
    return {
        # Pipeline lancé automatiquement par Airflow (toutes les 6h)
        "__default__": preprocessing_pipeline() + tfidf_pipeline() + credibility_scoring.create_pipeline(),
        # Pipelines individuels (lancement manuel)
        "preprocessing": preprocessing_pipeline(),
        "tfidf": tfidf_pipeline(),
        "fake_news_model": fake_news_model.create_pipeline(),
        "emotion_analysis": emotion_analysis.create_pipeline(),
        "credibility_scoring": credibility_scoring.create_pipeline(),
    }