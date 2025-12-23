"""
Visualisations Plotly "pures".
Chaque fonction retourne une figure Plotly (return fig).
Aucun display, aucun fig.show().
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from helpers import (
    filtrer_sport,
    agregation_licences_par_annee,
    pivot_licences_tranche_age,
    part_sous_population,
)


def graphique_licences_et_medailles(df: pd.DataFrame, sport: str = "all"):
    """
    Graphique interactif combiné :
    - x : années (celles présentes dans les données de licences)
    - y1 : nombre de licenciés (courbe)
    - y2 : médailles (barres empilées or/argent/bronze)
      affichées uniquement aux années JO (2016, 2021, 2024) via un mapping
      depuis les colonnes médailles (2016, 2020, 2024).

    Paramètres
    ----------
    df : pandas.DataFrame
        Base contenant au minimum : 'annee', 'sport', 'licences_annuelles'
        + colonnes médailles : '2016_or', '2020_or', '2024_or', etc.
    sport : str
        "all" ou un libellé exact de sport.
    """
    d = filtrer_sport(df, sport)
    lic = agregation_licences_par_annee(d)

    years = lic["annee"].tolist()
    medal_types = ("or", "argent", "bronze")
    medal_colors = {"or": "#F2C300", "argent": "#B0B0B0", "bronze": "#8C6239"}

    medals_by_year = {y: {m: 0 for m in medal_types} for y in years}

    if sport != "all":
        cols = ["sport"] + [f"{y}_{m}" for y in [2016, 2020, 2024] for m in medal_types]
        cols = [c for c in cols if c in df.columns]
        row = df[cols].drop_duplicates("sport")
        row = row[row["sport"] == sport]
        if not row.empty:
            row = row.iloc[0]
            mapping = {2016: 2016, 2020: 2021, 2024: 2024}
            for y, y_disp in mapping.items():
                if y_disp in medals_by_year:
                    for m in medal_types:
                        medals_by_year[y_disp][m] = int(row.get(f"{y}_{m}", 0) or 0)

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=years,
            y=lic["licences_annuelles"],
            mode="lines+markers",
            name="Licenciés",
        ),
        secondary_y=False,
    )

    for m in medal_types:
        fig.add_trace(
            go.Bar(
                x=years,
                y=[medals_by_year[y][m] for y in years],
                name=m.capitalize(),
                marker_color=medal_colors[m],
            ),
            secondary_y=True,
        )

    fig.update_layout(
        title=f"Évolution des licenciés et médailles – {sport}",
        barmode="stack",
        hovermode="x unified",
        width=1100,
        height=600,
    )
    fig.update_xaxes(title="Année")
    fig.update_yaxes(title="Licenciés", secondary_y=False)
    fig.update_yaxes(title="Médailles", secondary_y=True)

    return fig


def licences_par_annee(df: pd.DataFrame, sport_code="all", sport_col="code_sport"):
    """TODO"""
    d = (
        df
        if sport_code == "all"
        else df[
            df[sport_col].isin(
                [sport_code] if isinstance(sport_code, str) else sport_code
            )
        ]
    )
    agg = agregation_licences_par_annee(d)

    fig = px.line(
        agg,
        x="annee",
        y="licences_annuelles",
        markers=True,
        title="Licences par année",
    )
    fig.update_xaxes(dtick=1)
    return fig


def plot_part_femmes(df: pd.DataFrame, sport="all"):
    """TODO"""
    d = filtrer_sport(df, sport)
    sexe = d["sexe"].astype(str).str.lower()
    cond = sexe.str.startswith("f") | sexe.str.contains("fem")

    part = part_sous_population(d, cond)
    fig = px.line(
        part,
        x="annee",
        y="part",
        markers=True,
        title=f"Part des femmes – {sport}",
    )
    fig.update_yaxes(tickformat=".0%")
    fig.update_xaxes(dtick=1)
    return fig


def plot_part_jeunes(df: pd.DataFrame, age_max=15, sport="all"):
    """TODO"""
    d = filtrer_sport(df, sport)
    d["age"] = pd.to_numeric(d["age"], errors="coerce")
    cond = d["age"] < age_max

    part = part_sous_population(d, cond)
    fig = px.line(
        part,
        x="annee",
        y="part",
        markers=True,
        title=f"Part des jeunes (< {age_max} ans) – {sport}",
    )
    fig.update_yaxes(tickformat=".0%")
    fig.update_xaxes(dtick=1)
    return fig


def plot_heatmap_nbr_licencies(df: pd.DataFrame, sport="all"):
    """TODO"""
    d = filtrer_sport(df, sport)
    pivot = pivot_licences_tranche_age(d)

    fig = px.imshow(
        pivot,
        aspect="auto",
        title=f"Heatmap licences – {sport}",
        labels={"x": "Année", "y": "Tranche d'âge", "color": "Licences"},
    )
    return fig
