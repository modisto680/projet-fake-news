import os
import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# ─── Config page ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Thumalien — Credibility Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Connexion MongoDB ────────────────────────────────────────────────────────
@st.cache_resource
def get_mongo_client():
    uri = os.environ.get("MONGO_URI", "")
    return MongoClient(uri)

@st.cache_data(ttl=300)  # cache 5 min
def load_scored_posts(limit: int = 5000):
    client = get_mongo_client()
    db_name = os.environ.get("MONGO_DB", "thumalien")
    col = client[db_name]["scored_posts"]
    docs = list(col.find(
        {},
        {
            "_id": 0,
            "uri": 1,
            "author": 1,
            "indexedAt": 1,
            "text_clean": 1,
            "is_fake": 1,
            "fake_proba": 1,
            "credibility_score": 1,
            "scored_at": 1,
            "_source": 1,
        }
    ).sort("scored_at", -1).limit(limit))
    df = pd.DataFrame(docs)
    if not df.empty and "indexedAt" in df.columns:
        df["indexedAt"] = pd.to_datetime(df["indexedAt"], errors="coerce")
    return df

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/8/8c/Bluesky_Logo.svg/240px-Bluesky_Logo.svg.png", width=120)
    st.title("Thumalien")
    st.caption("Analyse de crédibilité des posts Bluesky")
    st.divider()

    limit = st.slider("Nombre de posts à charger", 500, 10000, 3000, step=500)

    score_min, score_max = st.slider(
        "Filtre : score de crédibilité",
        0.0, 1.0, (0.0, 1.0), step=0.01
    )

    show_fake_only = st.checkbox("Afficher uniquement les fakes", value=False)
    show_credible_only = st.checkbox("Afficher uniquement les crédibles", value=False)

    st.divider()
    if st.button("🔄 Rafraîchir les données"):
        st.cache_data.clear()

# ─── Chargement ───────────────────────────────────────────────────────────────
with st.spinner("Chargement des données depuis MongoDB..."):
    df = load_scored_posts(limit)

if df.empty:
    st.error("❌ Aucun post trouvé dans `scored_posts`. Lancez d'abord le pipeline `credibility_scoring`.")
    st.stop()

# ─── Filtrage ─────────────────────────────────────────────────────────────────
df_filtered = df[
    (df["credibility_score"] >= score_min) &
    (df["credibility_score"] <= score_max)
]
if show_fake_only:
    df_filtered = df_filtered[df_filtered["is_fake"] == True]
if show_credible_only:
    df_filtered = df_filtered[df_filtered["is_fake"] == False]

# ─── Header ───────────────────────────────────────────────────────────────────
st.title("🔍 Thumalien — Tableau de bord de crédibilité")
st.caption(f"Dernière mise à jour : {datetime.now().strftime('%d/%m/%Y %H:%M')}")
st.divider()

# ─── KPIs ─────────────────────────────────────────────────────────────────────
total = len(df)
n_fake = int(df["is_fake"].sum())
n_credible = total - n_fake
avg_score = df["credibility_score"].mean()

col1, col2, col3, col4 = st.columns(4)
col1.metric("📦 Posts analysés", f"{total:,}")
col2.metric("⚠️ Détectés fake", f"{n_fake:,}", f"{100*n_fake/total:.1f}%")
col3.metric("✅ Crédibles", f"{n_credible:,}", f"{100*n_credible/total:.1f}%")
col4.metric("📊 Score moyen", f"{avg_score:.3f}")

st.divider()

# ─── Graphiques ───────────────────────────────────────────────────────────────
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Distribution des scores de crédibilité")
    fig_hist = px.histogram(
        df,
        x="credibility_score",
        nbins=50,
        color_discrete_sequence=["#4C78A8"],
        labels={"credibility_score": "Score de crédibilité"},
    )
    fig_hist.add_vline(x=0.5, line_dash="dash", line_color="red", annotation_text="Seuil 0.5")
    fig_hist.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )
    st.plotly_chart(fig_hist, use_container_width=True)

with chart_col2:
    st.subheader("Répartition Fake vs Crédible")
    counts = df["is_fake"].value_counts().reset_index()
    counts.columns = ["is_fake", "count"]
    counts["label"] = counts["is_fake"].map({True: "Fake", False: "Crédible"})
    fig_pie = px.pie(
        counts,
        values="count",
        names="label",
        color="label",
        color_discrete_map={"Fake": "#E45756", "Crédible": "#54A24B"},
        hole=0.4,
    )
    fig_pie.update_layout(paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_pie, use_container_width=True)

# Évolution temporelle (si indexedAt disponible)
if "indexedAt" in df.columns and df["indexedAt"].notna().sum() > 10:
    st.subheader("📅 Évolution temporelle du score de crédibilité")
    df_time = df.dropna(subset=["indexedAt"]).copy()
    df_time["date"] = df_time["indexedAt"].dt.date
    daily = df_time.groupby("date").agg(
        avg_score=("credibility_score", "mean"),
        n_posts=("credibility_score", "count"),
    ).reset_index()
    fig_line = px.line(
        daily, x="date", y="avg_score",
        labels={"avg_score": "Score moyen", "date": "Date"},
        markers=True,
        color_discrete_sequence=["#4C78A8"],
    )
    fig_line.add_hline(y=0.5, line_dash="dash", line_color="red")
    fig_line.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig_line, use_container_width=True)

st.divider()

# ─── Tableau des posts ────────────────────────────────────────────────────────
st.subheader(f"📋 Posts filtrés ({len(df_filtered):,} résultats)")

display_cols = ["author", "text_clean", "credibility_score", "fake_proba", "is_fake", "indexedAt"]
display_cols = [c for c in display_cols if c in df_filtered.columns]

def color_score(val):
    if isinstance(val, float):
        if val < 0.3:
            return "background-color: #ffcccc"
        elif val > 0.7:
            return "background-color: #ccffcc"
    return ""

st.dataframe(
    df_filtered[display_cols]
    .sort_values("credibility_score", ascending=True)
    .reset_index(drop=True),
    use_container_width=True,
    height=400,
    column_config={
        "credibility_score": st.column_config.ProgressColumn(
            "Score crédibilité",
            format="%.3f",
            min_value=0,
            max_value=1,
        ),
        "fake_proba": st.column_config.NumberColumn("Proba fake", format="%.3f"),
        "is_fake": st.column_config.CheckboxColumn("Fake ?"),
        "text_clean": st.column_config.TextColumn("Texte", width="large"),
        "author": st.column_config.TextColumn("Auteur"),
        "indexedAt": st.column_config.DatetimeColumn("Date"),
    },
)

# ─── Green IT (CodeCarbon) ───────────────────────────────────────────────────
st.header("🍃 Espace Green IT & Responsabilité")
st.caption("Suivi de l'empreinte carbone du pipeline Thumalien via CodeCarbon")

@st.cache_data(ttl=300)
def load_emissions():
    path = "data/08_reporting/emissions.csv"
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()

em_df = load_emissions()

if not em_df.empty:
    # Calculs totaux
    total_co2 = em_df["emissions"].sum() # en kg
    total_energy = em_df["energy_consumed"].sum() # en kWh
    
    green_col1, green_col2, green_col3 = st.columns(3)
    green_col1.metric("🌍 Émissions totales", f"{total_co2:.6f} kg CO2eq")
    green_col2.metric("⚡ Énergie consommée", f"{total_energy:.6f} kWh")
    
    # Équivalence fun (ex: km en voiture) - 0.12 kg CO2 / km
    km_equiv = (total_co2 / 0.12) * 1000 # en mètres
    green_col3.metric("🚗 Équivalence trajet", f"{km_equiv:.2f} m en voiture")

    with st.expander("🔎 Détails techniques des émissions"):
        st.dataframe(em_df[["timestamp", "project_name", "duration", "emissions", "cpu_model"]], use_container_width=True)
else:
    st.info("ℹ️ Les données d'émissions seront disponibles après la prochaine exécution du pipeline.")

# ─── Footer ───────────────────────────────────────────────────────────────────
st.divider()
st.caption("Thumalien · Projet Mastère 1 Data & IA · Analyse des posts Bluesky")
