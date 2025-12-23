"""
Visualisations Plotly "pures".
Chaque fonction retourne une figure Plotly (return fig).
Aucun display, aucun fig.show().
"""

import re
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from helpers import (
    filtrer_sport,
    agregation_licences_par_annee,
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
    """
    Construit un graphique Plotly du nombre de licences annuelles (agrégé).

    Paramètres
    ----------
    df : pd.DataFrame
        Colonnes requises : ['annee', 'licences_annuelles', sport_col]
    sport_code : str | list[str] | "all"
        "all" pour tous sports, sinon un code sport (ou liste de codes).
    sport_col : str
        Nom de la colonne contenant le code sport.

    Retour
    ------
    plotly.graph_objects.Figure
    """

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


def evolution_licencies_age(df, age="all"):
    """
    Trace l'évolution du nombre de licenciés par sport, en filtrant éventuellement un âge.

    Paramètres
    ----------
    df : pandas.DataFrame
        Base complète des licenciés, contenant au moins :
        `annee`, `sport`, `licences_annuelles`, `code_sport`, `age`.
    age : str, optionnel
        Âge exact à filtrer (ex: "12"). Utiliser "all" pour tous les âges.

    Retour
    ------
    Graphique interactif Plotly représentant le nombre de licenciés par sport
    et par année pour l'âge sélectionné.
    """
    # Suppression des entrées "DIV" (divers) pour se concentrer sur les sports identifiés
    df_clean = df[df["code_sport"] != "DIV"]

    # Filtrage par âge
    if age != "all":
        df_filtre = df_clean[df_clean["age"] == age]
        titre_age = f"{age} ans"
    else:
        df_filtre = df_clean
        titre_age = "tous les âges"

    # Agrégation des licences par année et par sport
    table = (
        df_filtre.groupby(["annee", "sport"])["licences_annuelles"]
        .sum()
        .unstack()
        .sort_index()
    )

    # Passage en table large puis format long pour Plotly
    table_long = table.reset_index().melt(
        id_vars="annee", var_name="sport", value_name="licences_annuelles"
    )

    # Tracé
    fig = px.line(
        table_long,
        x="annee",
        y="licences_annuelles",
        color="sport",
        color_discrete_sequence=px.colors.qualitative.Alphabet,
        markers=True,
        labels={
            "annee": "Année",
            "licences_annuelles": "Nombre de licenciés",
            "sport": "Sport",
        },
        title=f"Évolution du nombre de licenciés de {titre_age} par sport",
    )

    # Mise en forme du graphique
    fig.update_layout(width=1100, height=600)

    # Affichage
    return fig


def evolution_licences_tranches_fines_age(df, tranche="all"):
    """
    Trace l'évolution du nombre de licenciés par sport,
    en filtrant éventuellement une tranche d'âge fine.

    Paramètres
    ----------
    df : pandas.DataFrame
        Base complète contenant `annee`, `sport`, `licences_annuelles`,
        `code_sport`, `tranche_age`.
    tranche : str, optionnel
        Tranche d'âge fine à filtrer (valeur de `tranche_age`).
        Utiliser "all" pour toutes les tranches.

    Retour
    ------
    Graphique interactif Plotly représentant le nombre de licenciés par sport
    et par année pour la tranche d'âge sélectionnée.
    """
    # Suppression des entrées "DIV" (divers) pour se concentrer sur les sports identifiés
    df_clean = df[df["code_sport"] != "DIV"]

    # Filtrage par tranche d'âge
    if tranche != "all":
        df_filtre = df_clean[df_clean["tranche_age"] == tranche]
        # Extraction du libellé de la tranche pour le titre
        titre_age = df_filtre["tranche_age"].str[4:].unique()[0]
    else:
        df_filtre = df_clean
        titre_age = "de toutes les tranches d'âge"

    df_filtre = df_filtre.sort_values(["annee", "sport"])

    # Agrégation des licences par année et par sport
    table = (
        df_filtre.groupby(["annee", "sport"])["licences_annuelles"]
        .sum()
        .unstack()
        .sort_index()
    )

    # Passage en long pour Plotly
    table_long = table.reset_index().melt(
        id_vars="annee", var_name="sport", value_name="licences_annuelles"
    )

    # Tracé
    fig = px.line(
        table_long,
        x="annee",
        y="licences_annuelles",
        color="sport",
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Alphabet,
        labels={
            "annee": "Année",
            "licences_annuelles": "Nombre de licenciés",
            "sport": "Sport",
        },
        title=f"Évolution du nombre de licenciés {titre_age} par sport",
    )

    # Mise en forme du graphique
    fig.update_layout(width=1200, height=650)

    # Affichage
    return fig


def evolution_licences_tranches_grandes_age(df, tranche="all"):
    """
    Trace l'évolution du nombre de licenciés par sport,
    en filtrant par une grande tranche d'âge.

    Paramètres
    ----------
    df : pandas.DataFrame
        Base complète contenant `annee`, `sport`, `licences_annuelles`,
        `code_sport`, `grande_tranche_age`.
    tranche : str, optionnel
        Grande tranche d'âge à filtrer (valeur de `grande_tranche_age`).
        Utiliser "all" pour toutes les tranches.

    Retour
    ------
    Graphique interactif Plotly représentant le nombre de licenciés par sport
    et par année pour la tranche d'âge sélectionnée.
    """
    # Suppression des entrées "DIV" (divers) pour se concentrer sur les sports identifiés
    df_clean = df[df["code_sport"] != "DIV"]

    # Filtrage par tranche d'âge
    if tranche != "all":
        df_filtre = df_clean[df_clean["grande_tranche_age"] == tranche]
        # Extraction du libellé de la tranche pour le titre
        titre_age = df_filtre["grande_tranche_age"].str[4:].unique()[0]
    else:
        df_filtre = df_clean
        titre_age = "de toutes les tranches d'âge"

    df_filtre = df_filtre.sort_values(["annee", "sport"])

    # Agrégation des licences par année et par sport
    table = (
        df_filtre.groupby(["annee", "sport"])["licences_annuelles"]
        .sum()
        .unstack()
        .sort_index()
    )

    # Passage en long pour Plotly
    table_long = table.reset_index().melt(
        id_vars="annee", var_name="sport", value_name="licences_annuelles"
    )

    # Tracé
    fig = px.line(
        table_long,
        x="annee",
        y="licences_annuelles",
        color="sport",
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Alphabet,
        labels={
            "annee": "Année",
            "licences_annuelles": "Nombre de licenciés",
            "sport": "Sport",
        },
        title=f"Évolution du nombre de licenciés {titre_age} par sport",
    )

    # Mise en forme du graphique
    fig.update_layout(width=1200, height=650)

    # Affichage
    return fig


def repartition_grandes_tranches_age_par_sport(df, annee="all"):
    """
    Affiche la répartition (en %) des licenciés par sport et grande tranche d'âge.

    Pour chaque sport, on calcule la proportion de licenciés dans chaque
    grande tranche d'âge (stacked bar chart).

    Paramètres
    ----------
    df : pandas.DataFrame
        Base complète contenant `sport`, `grande_tranche_age`, `licences_annuelles`,
        et `annee` si filtrage.
    annee : int ou str, optionnel
        Année à filtrer. Utiliser "all" pour agréger toutes les années.

    Retour
    ------
    None
        Affiche le graphique Plotly.
    """
    df_clean = df if annee == "all" else df[df["annee"] == annee]
    titre_an = "2016-2024" if annee == "all" else annee

    # Table sport x tranche -> effectifs
    df_pivot = df_clean.pivot_table(
        index="sport",
        columns="grande_tranche_age",
        values="licences_annuelles",
        aggfunc="sum",
        fill_value=0,
    )

    # Conversion en proportions (normalisation par sport)
    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0)

    # Format long pour Plotly
    df_long = df_prop.reset_index().melt(
        id_vars="sport", var_name="grande_tranche_age", value_name="proportion"
    )
    df_long["proportion"] = df_long["proportion"] * 100

    # Palette : on garde NR en noir (choix visuel explicite)
    tranches = sorted(df_long["grande_tranche_age"].unique())
    tranches_no_nr = [t for t in tranches if t != "NR - Non réparti"]
    n = len(tranches_no_nr)

    colors = px.colors.sample_colorscale(
        px.colors.sequential.Plasma_r,
        [i / (n - 1) for i in range(n)] if n > 1 else [0.5],
    )
    palette_map = {t: c for t, c in zip(tranches_no_nr, colors)}
    if "NR - Non réparti" in tranches:
        palette_map["NR - Non réparti"] = "black"

    fig = px.bar(
        df_long,
        x="proportion",
        y="sport",
        color="grande_tranche_age",
        color_discrete_map=palette_map,
        orientation="h",
        barmode="stack",
        labels={
            "proportion": "Proportion de licenciés (%)",
            "sport": "Sport",
            "grande_tranche_age": "Tranche d'âge",
        },
        title=f"Répartition des licenciés par sport et grande tranche d'âge – {titre_an}",
    )
    fig.update_xaxes(ticksuffix="%")
    fig.update_layout(width=1000, height=800, xaxis=dict(range=[0, 100]))
    return fig


def repartition_fines_tranches_age_par_sport(df, annee="all"):
    """
    Affiche la répartition (en %) des licenciés par sport et tranche d'âge fine.

    Paramètres
    ----------
    df : pandas.DataFrame
        Base complète contenant `sport`, `tranche_age`, `licences_annuelles`,
        et `annee` si filtrage.
    annee : int ou str, optionnel
        Année à filtrer. Utiliser "all" pour agréger toutes les années.

    Retour
    ------
    Graphique interactif Plotly représentant la répartition proportionnelle des licenciés
    par grande tranche d'âge pour chaque sport.
    """
    # Filtrage selon l'année
    df_clean = df if annee == "all" else df[df["annee"] == annee]
    titre_annee = "2016-2024" if annee == "all" else annee

    # Pivot pour obtenir les effectifs par sport et grande tranche d'âge
    df_pivot = df_clean.pivot_table(
        index="sport",
        columns="tranche_age",
        values="licences_annuelles",
        aggfunc="sum",
        fill_value=0,
    )

    # Conversion en proportions et gestion des divisions par zéro
    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0).fillna(0)

    # Transformation en format long pour Plotly
    df_long = df_prop.reset_index().melt(
        id_vars="sport", var_name="tranche_age", value_name="proportion"
    )
    df_long = df_long[df_long["tranche_age"].notna()]
    df_long["tranche_age"] = df_long["tranche_age"].astype(str)

    # Conversion en pourcentage
    df_long["proportion"] = df_long["proportion"] * 100

    # Palette + ordre des catégories : NR à la fin (si présent)
    tranches = sorted(df_long["tranche_age"].unique())
    tranches_no_nr = [t for t in tranches if t != "NR - Non réparti"]
    n = len(tranches_no_nr)

    colors = px.colors.sample_colorscale(
        px.colors.sequential.Plasma_r,
        [i / (n - 1) for i in range(n)] if n > 1 else [0.5],
    )

    # Assigner les couleurs aux tranches (et NR en noir)
    palette_map = {t: c for t, c in zip(tranches_no_nr, colors)}
    if "NR - Non réparti" in tranches:
        palette_map["NR - Non réparti"] = "black"

    tranches_ord = sorted(tranches_no_nr)
    if "NR - Non réparti" in tranches:
        tranches_ord.append("NR - Non réparti")

    # Tracé
    fig = px.bar(
        df_long,
        x="proportion",
        y="sport",
        color="tranche_age",
        color_discrete_map=palette_map,
        category_orders={"tranche_age": tranches_ord},
        orientation="h",
        barmode="stack",
        labels={
            "proportion": "Proportion de licenciés (%)",
            "sport": "Sport",
            "tranche_age": "Tranche d'âge",
        },
        title=f"Répartition des licenciés par sport et tranche d'âge fine – {titre_annee}",
    )

    # Mise en forme de l'axe x et des dimensions
    fig.update_xaxes(ticksuffix="%")
    fig.update_layout(width=1000, height=800, xaxis=dict(range=[1, 100]))

    # Affichage
    return fig


def plot_licences_par_sexe(
    df_lic: pd.DataFrame, sport_code: str = "all", sport_col: str = "code_sport"
):
    """
    Construit une figure Plotly des licences annuelles par sexe.

    Paramètres
    ----------
    df_lic : pd.DataFrame
        Doit contenir au minimum : ['annee', 'licences_annuelles', 'sexe', 'sport', sport_col].
    sport_code : str, optionnel
        "all" pour tous sports, sinon un code sport (ex "HAN").
    sport_col : str, optionnel
        Nom de la colonne contenant le code sport.

    Retour
    ------
    plotly.graph_objects.Figure
        Courbes (ou points) par sexe, en fonction de l'année.
    """
    df = df_lic.copy()
    if sport_code != "all":
        df = df[df[sport_col] == sport_code]

    # Agrégation
    agg = (
        df.groupby(["annee", "sexe"], as_index=False)["licences_annuelles"]
        .sum()
        .sort_values(["annee", "sexe"])
    )

    titre = "Tous sports" if sport_code == "all" else sport_code
    fig = px.line(
        agg,
        x="annee",
        y="licences_annuelles",
        color="sexe",
        markers=True,
        title=f"Licences annuelles par sexe – {titre}",
        labels={
            "annee": "Année",
            "licences_annuelles": "Licences annuelles",
            "sexe": "Sexe",
        },
    )
    fig.update_xaxes(dtick=1)
    return fig


def plot_part_jeunes(
    df_lic: pd.DataFrame,
    age_max: int = 15,
    sport_code: str = "all",
    sport_col: str = "code_sport",
):
    """
    Construit une figure Plotly de la part de jeunes (< age_max) parmi les licenciés, par année.

    Paramètres
    ----------
    df_lic : pd.DataFrame
        Colonnes attendues : ['annee', 'licences_annuelles', 'age', 'sport', sport_col].
    age_max : int, optionnel
        Seuil strict : jeunes si age < age_max.
    sport_code : str, optionnel
        "all" pour tous sports, sinon un code sport.
    sport_col : str, optionnel
        Nom de la colonne contenant le code sport.

    Retour
    ------
    plotly.graph_objects.Figure
        Courbe de la part des jeunes (%) par année.
    """
    df = df_lic.copy()
    df["age"] = pd.to_numeric(df["age"], errors="coerce")

    if sport_code != "all":
        df = df[df[sport_col] == sport_code]

    total = (
        df.groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "licences_total"})
    )
    jeunes = (
        df[df["age"] < age_max]
        .groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "licences_jeunes"})
    )

    merged = total.merge(jeunes, on="annee", how="left").fillna({"licences_jeunes": 0})
    merged["part_jeunes"] = np.where(
        merged["licences_total"] > 0,
        merged["licences_jeunes"] / merged["licences_total"],
        np.nan,
    )

    titre = "Tous sports" if sport_code == "all" else sport_code
    fig = px.line(
        merged.sort_values("annee"),
        x="annee",
        y="part_jeunes",
        markers=True,
        title=f"Part des jeunes (< {age_max} ans) – {titre}",
        labels={"annee": "Année", "part_jeunes": "Part des jeunes"},
    )
    fig.update_yaxes(tickformat=".0%")
    fig.update_xaxes(dtick=1)
    return fig


def plot_part_femmes(
    df_lic: pd.DataFrame, sport_code: str = "all", sport_col: str = "code_sport"
):
    """
    Construit une figure Plotly de la part des femmes parmi les licenciés, par année.

    Paramètres
    ----------
    df_lic : pd.DataFrame
        Colonnes attendues : ['annee', 'licences_annuelles', 'sexe', 'sport', sport_col].
    sport_code : str, optionnel
        "all" pour tous sports, sinon un code sport.
    sport_col : str, optionnel
        Nom de la colonne contenant le code sport.

    Retour
    ------
    plotly.graph_objects.Figure
        Courbe de la part des femmes (%) par année.
    """
    df = df_lic.copy()
    if sport_code != "all":
        df = df[df[sport_col] == sport_code]

    sexe = df["sexe"].astype(str).str.lower()
    is_femme = sexe.str.startswith("f") | sexe.str.contains("fem")

    total = (
        df.groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "licences_total"})
    )
    femmes = (
        df[is_femme]
        .groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "licences_femmes"})
    )

    merged = total.merge(femmes, on="annee", how="left").fillna({"licences_femmes": 0})
    merged["part_femmes"] = np.where(
        merged["licences_total"] > 0,
        merged["licences_femmes"] / merged["licences_total"],
        np.nan,
    )

    titre = "Tous sports" if sport_code == "all" else sport_code
    fig = px.line(
        merged.sort_values("annee"),
        x="annee",
        y="part_femmes",
        markers=True,
        title=f"Part des femmes – {titre}",
        labels={"annee": "Année", "part_femmes": "Part des femmes"},
    )
    fig.update_yaxes(tickformat=".0%")
    fig.update_xaxes(dtick=1)
    return fig


def plot_heatmap_nbr_licencies(
    df_lic: pd.DataFrame, sport_code: str = "all", sport_col: str = "code_sport"
):
    """
    Construit une heatmap Plotly du nombre de licences par tranche d'âge et par année.

    Paramètres
    ----------
    df_lic : pd.DataFrame
        Colonnes attendues : ['annee', 'tranche_age', 'licences_annuelles', 'sport', sport_col].
    sport_code : str, optionnel
        "all" pour tous sports, sinon un code sport.
    sport_col : str, optionnel
        Nom de la colonne contenant le code sport.

    Retour
    ------
    plotly.graph_objects.Figure
        Heatmap interactive (années en x, tranches d'âge en y).
    """
    df = df_lic.copy()
    if sport_code != "all":
        df = df[df[sport_col] == sport_code]

    # Pivot : lignes = tranche_age, colonnes = annee
    pivot = df.pivot_table(
        index="tranche_age",
        columns="annee",
        values="licences_annuelles",
        aggfunc="sum",
        fill_value=0,
    ).sort_index()

    def _lower_bound(x):
        m = re.search(r"\d+", str(x))
        return int(m.group()) if m else 10**9

    pivot = pivot.loc[sorted(pivot.index, key=_lower_bound), :].sort_index(axis=1)

    titre = "Tous sports" if sport_code == "all" else sport_code
    fig = px.imshow(
        pivot,
        aspect="auto",
        title=f"Heatmap licences (tranche d'âge × année) – {titre}",
        labels={"x": "Année", "y": "Tranche d'âge", "color": "Licences"},
    )
    return fig
