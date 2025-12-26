"""
Visualisations Plotly "pures".
Chaque fonction retourne une figure Plotly (return fig).
"""

import re
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .helpers import (
    filtrer_sport,
    agregation_licences_par_annee,
)


def graphique_licences_et_medailles(df, sport = "all"):
    """
    Construit un graphique Plotly combinant l'évolution des licenciés et des médailles.

    Paramètres
    ----------
    df : pandas.DataFrame
        Colonnes requises :'annee', 'sport', 'licences_annuelles',
        '{annee}_or', '{annee}_argent', '{annee}_bronze' pour annee ∈ {2016, 2020, 2024}
    sport : str, optionnel (par défaut = "all")

    Retour
    ------
    plotly.graph_objects.Figure
    """
    # Filtrage des données selon le sport puis agrégation annuelle des licenciés
    d = filtrer_sport(df, sport)
    lic = agregation_licences_par_annee(d)

    # Années affichées (celles disponibles dans la série de licenciés)
    years = lic["annee"].tolist()

    medal_types = ("or", "argent", "bronze")
    medal_colors = {"or": "#F2C300", "argent": "#B0B0B0", "bronze": "#8C6239"}

    # Initialisation : 0 médailles sur toutes les années
    medals_by_year = {y: {m: 0 for m in medal_types} for y in years}

    # Récupération des médailles uniquement si un sport spécifique est sélectionné
    if sport != "all":
        # Colonnes médailles potentiellement disponibles
        cols = ["sport"] + [f"{y}_{m}" for y in [2016, 2020, 2024] for m in medal_types]

        # On se ramène à une ligne par sport pour lire les colonnes médailles
        cols = [c for c in cols if c in df.columns]
        row = df[cols].drop_duplicates("sport")
        row = row[row["sport"] == sport]

        if not row.empty:
            row = row.iloc[0]

            # Mapping édition JO -> année d'affichage côté licences
            mapping = {2016: 2016, 2020: 2021, 2024: 2024}

            for y, y_disp in mapping.items():
                # On ne remplit que si l'année d'affichage existe dans l'axe des années
                if y_disp in medals_by_year:
                    for m in medal_types:
                        medals_by_year[y_disp][m] = int(row.get(f"{y}_{m}", 0) or 0)

    # Figure à deux axes y
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Barres empilées pour les médailles
    for m in medal_types:
        fig.add_trace(
            go.Bar(
                x=years,
                y=[medals_by_year[y][m] for y in years],
                name=m.capitalize(),
                marker_color=medal_colors[m],
                width=0.5
            ),
            secondary_y=True,
        )

    # Courbe des licenciés
    fig.add_trace(
        go.Scatter(
            x=years,
            y=lic["licences_annuelles"],
            mode="lines+markers",
            name="Licenciés",
        ),
        secondary_y=False,
    )

    # Mise en forme
    fig.update_layout(
        title=f"Évolution des licenciés et médailles – {sport}",
        barmode="stack",
        hovermode="x unified",
        width=1100,
        height=600
    )
    fig.update_xaxes(title="Année")
    fig.update_yaxes(title="Licenciés", secondary_y=False)
    fig.update_yaxes(title="Médailles", secondary_y=True)

    return fig


def licences_par_annee(df, sport_code="all", sport_col="code_sport"):
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
    # Filtrage éventuel sur un ou plusieurs codes sport
    d = (
        df
        if sport_code == "all"
        else df[
            df[sport_col].isin(
                [sport_code] if isinstance(sport_code, str) else sport_code
            )
        ]
    )

    # Agrégation annuelle
    agg = agregation_licences_par_annee(d)

    # Titre
    if sport_code == "all":
        titre_sport = "Tous sports"
    else:
        titre_sport = df.loc[df[sport_col] == sport_code, "sport"].iloc[0]

    # Tracé
    fig = px.line(
        agg,
        x="annee",
        y="licences_annuelles",
        markers=True,
        title=f"Nombre de licences par année — {titre_sport}",
        color_discrete_sequence=["darkorchid"],
        labels={
            "annee": "Année",
            "licences_annuelles": "Nombre de licenciés"
        },
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
        Âge exact à filtrer. Utiliser "all" pour tous les âges.

    Retour
    ------
    plotly.graph_objects.Figure
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

    Retour
    ------
    plotly.graph_objects.Figure
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

    Retour
    ------
    plotly.graph_objects.Figure
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

    Paramètres
    ----------
    df : pandas.DataFrame
        Base complète contenant `sport`, `grande_tranche_age`, `licences_annuelles`,
        et `annee` si filtrage.
    annee : int ou str, optionnel
        Année à filtrer. Utiliser "all" pour agréger toutes les années.

    Retour
    ------
    plotly.graph_objects.Figure
    """
    # Filtrage selon l'année
    df_clean = df if annee == "all" else df[df["annee"] == annee]
    titre_an = "2016-2024" if annee == "all" else annee

    # Pivot pour obtenir les effectifs par sport et grande tranche d'âge
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

    # Palette : on garde NR en noir
    tranches = sorted(df_long["grande_tranche_age"].unique())
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

    # Tracé
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

    # Mise en forme de l'axe x et des dimensions
    fig.update_xaxes(ticksuffix="%")
    fig.update_layout(width=1000, height=800, xaxis=dict(range=[0, 100]))

    # Affichage
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
    plotly.graph_objects.Figure
    """
    # Filtrage selon l'année
    df_clean = df if annee == "all" else df[df["annee"] == annee]
    titre_annee = "2016-2024" if annee == "all" else annee

    # Pivot pour obtenir les effectifs par sport et tranche d'âge fine
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

    # Palette + ordre des catégories : NR à la fin
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


def graphique_licences_par_sexe(df_lic, sport_code="all", sport_col="code_sport"):
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
    """
    df = df_lic.copy()

    # Tri selon le sport
    if sport_code != "all":
        df = df[df[sport_col] == sport_code]

    # Agrégation
    agg = (
        df.groupby(["annee", "sexe"], as_index=False)["licences_annuelles"]
        .sum()
        .sort_values(["annee", "sexe"])
    )

    # Harmonisation des libellés pour la légende
    sexe_map = {
        "F": "Femmes",
        "H": "Hommes",
        "NR - Non réparti": "Non réparti",
    }

    agg["sexe_label"] = agg["sexe"].map(sexe_map).fillna(agg["sexe"])

    # Palette de couleurs
    color_map = {
        "Femmes": "darkorchid",
        "Hommes": "orange",
        "Non réparti": "black",
    }

    # Titre
    titre = "Tous sports" if sport_code == "all" else sport_code

    # Tracé
    fig = px.line(
        agg,
        x="annee",
        y="licences_annuelles",
        color="sexe_label",
        markers=True,
        title=f"Licences annuelles par sexe – {titre}",
        color_discrete_map=color_map,
        labels={
            "annee": "Année",
            "licences_annuelles": "Nombre de licenciés",
            "sexe_label": "Sexe"
        },
    )

    fig.update_xaxes(dtick=1)

    # Affichage
    return fig


def graphique_part_jeunes(df_lic, age_max=15, sport_code="all", sport_col="code_sport"):
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
    """
    df = df_lic.copy()

    # Conversion de l'âge en numérique
    df["age"] = pd.to_numeric(df["age"], errors="coerce")

    # Filtrage par sport
    if sport_code != "all":
        df = df[df[sport_col] == sport_code]

    # Total des licences par année
    total = (
        df.groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "licences_total"})
    )

    # Total des licences de jeunes par année
    jeunes = (
        df[df["age"] < age_max]
        .groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "licences_jeunes"})
    )

    # Fusion et gestion des années sans jeunes
    merged = total.merge(jeunes, on="annee", how="left").fillna({"licences_jeunes": 0})

    # Calcul de la part de jeunes
    merged["part_jeunes"] = np.where(
        merged["licences_total"] > 0,
        merged["licences_jeunes"] / merged["licences_total"],
        np.nan,
    )

    # Titre
    if sport_code == "all":
        titre_sport = "Tous sports"
    else:
        titre_sport = df_lic.loc[df_lic[sport_col] == sport_code, "sport"].iloc[0]

    # Tracé
    fig = px.line(
        merged.sort_values("annee"),
        x="annee",
        y="part_jeunes",
        color_discrete_sequence=["darkorchid"],
        markers=True,
        title=f"Part des jeunes (< {age_max} ans) – {titre_sport}",
        labels={"annee": "Année", "part_jeunes": "Part des jeunes"},
    )

    fig.update_yaxes(tickformat=".0%")
    fig.update_xaxes(dtick=1)

    # Affichage
    return fig


def graphique_part_femmes(df_lic, sport_code="all", sport_col="code_sport"):
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
    """
    df = df_lic.copy()

    # Filtrage par sport
    if sport_code != "all":
        df = df[df[sport_col] == sport_code]

    # Normalisation de la colonne sexe et construction du masque "femme"
    sexe = df["sexe"].astype(str).str.lower()
    is_femme = sexe.str.startswith("f") | sexe.str.contains("fem")

    # Total des licences par année
    total = (
        df.groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "licences_total"})
    )

    # Total des licences féminines par année
    femmes = (
        df[is_femme]
        .groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "licences_femmes"})
    )

    # Fusion et gestion des années sans femmes
    merged = total.merge(femmes, on="annee", how="left").fillna({"licences_femmes": 0})

    # Calcul de la part
    merged["part_femmes"] = np.where(
        merged["licences_total"] > 0,
        merged["licences_femmes"] / merged["licences_total"],
        np.nan,
    )

    # Titre
    if sport_code == "all":
        titre_sport = "Tous sports"
    else:
        titre_sport = df_lic.loc[df_lic[sport_col] == sport_code, "sport"].iloc[0]


    # Tracé
    fig = px.line(
        merged.sort_values("annee"),
        x="annee",
        y="part_femmes",
        markers=True,
        color_discrete_sequence=["darkorchid"],
        title=f"Part des femmes – {titre_sport}",
        labels={"annee": "Année", "part_femmes": "Part des femmes"},
    )

    fig.update_yaxes(tickformat=".0%")
    fig.update_xaxes(dtick=1)

    # Affichage
    return fig


def heatmap_nbr_licencies(df_lic, sport_code="all", sport_col="code_sport"):
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
    """
    df = df_lic.copy()

    # Filtrage par sport
    if sport_code != "all":
        df = df[df[sport_col] == sport_code]

    # Table pivot :
    # lignes = tranche_age, colonnes = annee, valeurs = somme des licences
    pivot = df.pivot_table(
        index="tranche_age",
        columns="annee",
        values="licences_annuelles",
        aggfunc="sum",
        fill_value=0,
    ).sort_index()

    # Tri des tranches d'âge selon leur borne inférieure
    def _lower_bound(x):
        m = re.search(r"\d+", str(x))
        return int(m.group()) if m else 10**9

    pivot = pivot.loc[sorted(pivot.index, key=_lower_bound), :].sort_index(axis=1)

    # Titre
    if sport_code == "all":
        titre_sport = "Tous sports"
    else:
        titre_sport = df_lic.loc[df_lic[sport_col] == sport_code, "sport"].iloc[0]

    # Tracé
    fig = px.imshow(
        pivot,
        aspect="auto",
        title=f"Heatmap licences (tranche d'âge × année) – {titre_sport}",
        labels={"x": "Année", "y": "Tranche d'âge", "color": "Licences"},
        color_continuous_scale="matter"
    )

    return fig


def medailles_par_annee(df):
    """
    Construit une figure Plotly représentant le nombre de médailles olympiques
    remportées par la France par année de Jeux (2016, 2020, 2024) et par couleur.

    Paramètres
    ----------
    df : pandas.DataFrame

    Retour
    ------
    plotly.graph_objects.Figure
    """

    # Colonnes correspondant aux médailles par année et par couleur
    medal_cols = [
        "2016_or", "2016_argent", "2016_bronze",
        "2020_or", "2020_argent", "2020_bronze",
        "2024_or", "2024_argent", "2024_bronze",
    ]
    total_cols = [
        "total_medailles_2016",
        "total_medailles_2020",
        "total_medailles_2024",
    ]

    # Extraction des colonnes nécessaires
    cols = ["code_sport"] + medal_cols + total_cols
    tmp = df[cols].copy()

    # Exclusion des DIV
    tmp = tmp[tmp["code_sport"] != "DIV"]

    # Déduplication : une ligne par sport
    by_sport = tmp.groupby("code_sport", as_index=False).max(numeric_only=True)

    # Agrégation
    rows = []
    for y in [2016, 2020, 2024]:
        or_ = by_sport[f"{y}_or"].sum()
        argent = by_sport[f"{y}_argent"].sum()
        bronze = by_sport[f"{y}_bronze"].sum()

        # Utilisation du total pré-calculé si disponible
        total_col = f"total_medailles_{y}"
        total = (
            by_sport[total_col].sum()
            if total_col in by_sport.columns
            else or_ + argent + bronze
        )

        rows.append(
            {
                "annee": y,
                "Or": or_,
                "Argent": argent,
                "Bronze": bronze,
                "Total": total,
            }
        )

    out = pd.DataFrame(rows)

    # Passage au format long pour Plotly
    long_df = out.melt(
        id_vars="annee",
        value_vars=["Or", "Argent", "Bronze", "Total"],
        var_name="type_medaille",
        value_name="nb",
    )

    # Palette de couleurs
    color_map = {
        "Or": "#F2C300",
        "Argent": "#B0B0B0",
        "Bronze": "#8C6239",
        "Total": "black",
    }

    # Tracé
    fig = px.line(
        long_df,
        x="annee",
        y="nb",
        color="type_medaille",
        markers=True,
        color_discrete_map=color_map,
        title="Evolution du nombre de médailles olympiques remportées",
    )

    fig.update_layout(
        template="plotly_white",
        xaxis_title="Année des JO",
        yaxis_title="Nombre de médailles",
        legend_title_text="Type de médaille",
    )

    return fig


def camembert_medailles(df, annee_jo="all"):
    """
    Construit un camembert Plotly de la répartition Or / Argent / Bronze
    des médailles olympiques de la France.

    Paramètres
    ----------
    df : pd.DataFrame
        Base complète.
    annee_jo : int ou str, optionnel
        - 2016, 2020, 2024 : camembert pour une année donnée
        - "all" (défaut)   : agrégation sur toutes les années JO

    Retour
    ------
    plotly.graph_objects.Figure
    """
    annees_disponibles = [2016, 2020, 2024]

    # Détermination des années à utiliser selon le paramètre
    if annee_jo == "all":
        annees = annees_disponibles
        titre_annee = "JO 2016–2024"
    elif annee_jo in annees_disponibles:
        annees = [annee_jo]
        titre_annee = f"JO {annee_jo}"
    else:
        raise ValueError("annee_jo doit être 2016, 2020, 2024 ou 'all'.")

    # Colonnes médailles nécessaires
    medal_cols = []
    for y in annees:
        for m in ["or", "argent", "bronze"]:
            col = f"{y}_{m}"
            if col not in df.columns:
                raise ValueError(f"Colonne manquante : {col}")
            medal_cols.append(col)

    # Déduplication par sport + exclusion DIV
    tmp = df[df["code_sport"] != "DIV"][["code_sport"] + medal_cols].copy()
    by_sport = tmp.groupby("code_sport", as_index=False).max(numeric_only=True)

    # Agrégation
    or_total = sum(by_sport[f"{y}_or"].sum() for y in annees)
    argent_total = sum(by_sport[f"{y}_argent"].sum() for y in annees)
    bronze_total = sum(by_sport[f"{y}_bronze"].sum() for y in annees)
    total = or_total + argent_total + bronze_total

    # Données au format long pour le camembert
    df_pie = pd.DataFrame(
        {
            "type_medaille": ["Or", "Argent", "Bronze"],
            "nb": [or_total, argent_total, bronze_total],
        }
    )

    # Palette de couleurs
    color_map = {
        "Or": "#F2C300",
        "Argent": "#B0B0B0",
        "Bronze": "#8C6239",
    }

    # Tracé
    fig = px.pie(
        df_pie,
        names="type_medaille",
        values="nb",
        color="type_medaille",
        color_discrete_map=color_map,
        hole=0.35,
        title=f"Répartition des médailles ({titre_annee})<br>Total = {int(total)}",
    )

    fig.update_traces(textinfo="percent+value", textposition="inside")
    fig.update_layout(template="plotly_white", legend_title_text="Médaille")

    return fig
