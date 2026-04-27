import pickle
import pandas as pd
import numpy as np
import shap
import matplotlib.pyplot as plt
import os

def explain_text(text: str, model_path: str):
    print(f"\n--- Analyse SHAP pour le texte : ---")
    print(f"'{text}'")
    
    if not os.path.exists(model_path):
        print(f"Erreur : Le modèle est introuvable à {model_path}")
        return

    with open(model_path, 'rb') as f:
        pipeline = pickle.load(f)
    
    # Séparer vectorizer et classifieur
    tfidf = pipeline.named_steps['tfidf']
    clf = pipeline.named_steps['clf']
    
    # Créer un Explainer SHAP pour les modèles linéaires
    # On doit passer les données d'entraînement pour le "background", 
    # mais pour une régression logistique, on peut utiliser l'explainer linéaire directement.
    
    # Transformer le texte en vecteur TF-IDF
    X_vec = tfidf.transform([text])
    
    # Créer l'explainer linéaire
    explainer = shap.LinearExplainer(clf, tfidf.transform([""]*100)) # background neutre
    shap_values = explainer.shap_values(X_vec)
    
    # Récupérer les mots présents dans le texte
    feature_names = tfidf.get_feature_names_out()
    
    # On récupère les indices des mots non-nuls dans X_vec
    nonzero_indices = X_vec.nonzero()[1]
    
    results = []
    for idx in nonzero_indices:
        word = feature_names[idx]
        val = shap_values[0, idx]
        results.append((word, val))
    
    # Trier par importance absolue
    results.sort(key=lambda x: abs(x[1]), reverse=True)
    
    print("\n💡 CONTRIBUTION DES MOTS AU SCORE 'FAKE' :")
    print(f"{'MOT':<20} | {'CONTRIBUTION (SHAP)':<20} | {'EFFET'}")
    print("-" * 55)
    
    for word, val in results:
        effect = "🔴 POUSSE VERS FAKE" if val > 0 else "🔵 POUSSE VERS VRAI"
        print(f"{word:<20} | {val:>20.4f} | {effect}")

    # Calcul du score final (base + somme des contributions)
    expected_value = explainer.expected_value
    total_contribution = sum(val for _, val in results)
    final_score_logit = expected_value + total_contribution
    # Conversion logit -> probabilité (sigmoïde)
    prob_fake = 1 / (1 + np.exp(-final_score_logit))
    
    print("-" * 55)
    print(f"SCORE FINAL (Probabilité Fake) : {prob_fake:.2%}")

if __name__ == "__main__":
    import sys
    
    # Texte par défaut ou argument
    test_text = "BREAKING news from Washington: Reuters reports that image is fake news !!!"
    if len(sys.argv) > 1:
        test_text = " ".join(sys.argv[1:])
    
    model_file = "/opt/airflow/kedro/facts_check/data/06_models/fake_news_model.pkl"
    explain_text(test_text, model_file)
