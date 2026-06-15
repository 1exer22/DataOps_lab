import json
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

DB_PATH   = Path(__file__).parent / "warehouse.duckdb"
REPORT    = Path(__file__).parent / "last_run.json"

st.set_page_config(
    page_title="DataOps Dashboard",
    page_icon="📊",
    layout="wide",
)

# ── Rapport du dernier pipeline run ──────────────────────────────────────────

def read_last_run() -> dict | None:
    if REPORT.exists():
        return json.loads(REPORT.read_text(encoding="utf-8"))
    return None

# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_data
def load_orders() -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute("SELECT * FROM main_marts.mart_orders_summary").df()
    con.close()
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


@st.cache_data
def load_revenue_by_category() -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute("SELECT * FROM main_marts.mart_revenue_by_category").df()
    con.close()
    return df


orders_raw = load_orders()
rev_cat = load_revenue_by_category()

# ── Sidebar filters ───────────────────────────────────────────────────────────

with st.sidebar:
    st.title("Filtres")

    # ── Statut du dernier pipeline run ───────────────────────────────────────
    last = read_last_run()
    if last:
        color = "green" if last["status"] == "OK" else "red"
        st.markdown(f"**Dernier run :** :{color}[{last['status']}]")
        st.caption(last["last_run"])
        for step, status in last["steps"].items():
            icon = "✅" if status in ("OK", "SKIP") else ("⚠️" if step == "dbt_test" else "❌")
            st.caption(f"{icon} {step} : {status}")
    else:
        st.caption("Aucun run pipeline detecte.")

    if st.button("Rafraichir les donnees"):
        st.cache_data.clear()
        st.rerun()

    st.divider()

    date_min = orders_raw["order_date"].min().date()
    date_max = orders_raw["order_date"].max().date()
    date_range = st.date_input(
        "Période",
        value=(date_min, date_max),
        min_value=date_min,
        max_value=date_max,
    )

    countries = st.multiselect(
        "Pays client",
        options=sorted(orders_raw["customer_country"].unique()),
        default=sorted(orders_raw["customer_country"].unique()),
    )

    segments = st.multiselect(
        "Segment client",
        options=sorted(orders_raw["customer_segment"].unique()),
        default=sorted(orders_raw["customer_segment"].unique()),
    )

    statuses = st.multiselect(
        "Statut commande",
        options=sorted(orders_raw["status"].unique()),
        default=sorted(orders_raw["status"].unique()),
    )

# Apply filters
if len(date_range) == 2:
    d_start, d_end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
else:
    d_start, d_end = orders_raw["order_date"].min(), orders_raw["order_date"].max()

orders = orders_raw[
    (orders_raw["order_date"] >= d_start)
    & (orders_raw["order_date"] <= d_end)
    & (orders_raw["customer_country"].isin(countries))
    & (orders_raw["customer_segment"].isin(segments))
    & (orders_raw["status"].isin(statuses))
]

# ── Header ────────────────────────────────────────────────────────────────────

st.title("📊 DataOps Dashboard")
st.caption("Source : DuckDB · Transformations dbt · Données synthétiques")

if orders.empty:
    st.warning("Aucune donnée ne correspond aux filtres sélectionnés.")
    st.stop()

# ── KPIs ──────────────────────────────────────────────────────────────────────

total_revenue = orders["total_amount"].sum()
nb_orders = orders["order_id"].nunique()
avg_basket = orders["total_amount"].mean()
nb_customers = orders["customer_country"].count()  # proxy

k1, k2, k3, k4 = st.columns(4)
k1.metric("Chiffre d'affaires", f"{total_revenue:,.0f} €")
k2.metric("Commandes", f"{nb_orders:,}")
k3.metric("Panier moyen", f"{avg_basket:,.0f} €")
k4.metric("Produits / commande (moy.)", f"{orders['nb_products'].mean():.1f}")

st.divider()

# ── Row 1 : Évolution CA + Statuts ───────────────────────────────────────────

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("Évolution du CA")
    daily = (
        orders.groupby("order_date")
        .agg(revenue=("total_amount", "sum"), nb=("order_id", "count"))
        .reset_index()
    )
    fig_line = px.line(
        daily,
        x="order_date",
        y="revenue",
        labels={"order_date": "Date", "revenue": "CA (€)"},
        markers=True,
    )
    fig_line.update_traces(line_color="#4C78A8")
    st.plotly_chart(fig_line, use_container_width=True)

with col_right:
    st.subheader("Statut des commandes")
    status_df = orders["status"].value_counts().reset_index()
    status_df.columns = ["status", "count"]
    fig_pie = px.pie(
        status_df,
        names="status",
        values="count",
        color_discrete_sequence=px.colors.qualitative.Set2,
        hole=0.4,
    )
    fig_pie.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig_pie, use_container_width=True)

# ── Row 2 : Revenus par catégorie + Par pays ──────────────────────────────────

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Revenus par catégorie")
    fig_cat = px.bar(
        rev_cat.sort_values("total_revenue", ascending=True),
        x="total_revenue",
        y="product_category",
        orientation="h",
        labels={"total_revenue": "CA (€)", "product_category": "Catégorie"},
        color="product_category",
        color_discrete_sequence=px.colors.qualitative.Pastel,
        text_auto=".3s",
    )
    fig_cat.update_layout(showlegend=False)
    st.plotly_chart(fig_cat, use_container_width=True)

with col_b:
    st.subheader("CA par pays")
    country_df = (
        orders.groupby("customer_country")
        .agg(revenue=("total_amount", "sum"), nb_orders=("order_id", "count"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    fig_country = px.bar(
        country_df,
        x="customer_country",
        y="revenue",
        color="customer_country",
        labels={"customer_country": "Pays", "revenue": "CA (€)"},
        color_discrete_sequence=px.colors.qualitative.Bold,
        text_auto=".3s",
    )
    fig_country.update_layout(showlegend=False)
    st.plotly_chart(fig_country, use_container_width=True)

# ── Row 3 : Segment ───────────────────────────────────────────────────────────

st.subheader("CA et nombre de commandes par segment")
seg_df = (
    orders.groupby("customer_segment")
    .agg(revenue=("total_amount", "sum"), nb_orders=("order_id", "count"))
    .reset_index()
)
fig_seg = px.bar(
    seg_df,
    x="customer_segment",
    y=["revenue", "nb_orders"],
    barmode="group",
    labels={"customer_segment": "Segment", "value": "Valeur", "variable": "Métrique"},
    color_discrete_sequence=["#4C78A8", "#F58518"],
)
st.plotly_chart(fig_seg, use_container_width=True)

# ── Raw data table ────────────────────────────────────────────────────────────

with st.expander("📋 Données brutes — commandes"):
    st.dataframe(
        orders.sort_values("order_date", ascending=False),
        use_container_width=True,
        hide_index=True,
    )
