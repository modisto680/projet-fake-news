import pickle
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline

def diagnose_model(model_path: str):
    print(f"--- Diagnostic du modèle : {model_path} ---")
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    # Extraire le vectorizer et le classifieur de la pipeline
    tfidf = model.named_steps['tfidf']
    clf = model.named_steps['clf']
    
    # Récupérer les noms des mots (features)
    feature_names = tfidf.get_feature_names_out()
    
    # Récupérer les coefficients (poids)
    # Pour la régression logistique binaire, clf.coef_[0] contient les poids
    coeffs = clf.coef_[0]
    
    # Créer un DataFrame pour trier facilement
    df_weights = pd.DataFrame({
        'word': feature_names,
        'weight': coeffs
    })
    
    # Top 20 Fake (Poids positifs les plus élevés)
    top_fake = df_weights.sort_values(by='weight', ascending=False).head(20)
    
    # Top 20 True (Poids négatifs les plus élevés)
    top_true = df_weights.sort_values(by='weight', ascending=True).head(20)
    
    print("\n🚨 TOP 20 MOTS 'FAKE' (Poussent vers le label 1) :")
    print(top_fake.to_string(index=False))
    
    print("\n✅ TOP 20 MOTS 'VRAIS' (Poussent vers le label 0) :")
    print(top_true.to_string(index=False))

if __name__ == "__main__":
    model_file = "/opt/airflow/kedro/facts_check/data/06_models/fake_news_model.pkl"
    diagnose_model(model_file)
