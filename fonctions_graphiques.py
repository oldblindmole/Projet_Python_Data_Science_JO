"""
Module de visualisation interactive des données de licenciés.

Ce module est conçu pour être importé et utilisé depuis le notebook
principal (main.ipynb).
"""

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import plotly.express as px
import ipywidgets as widgets
from IPython.display import display, clear_output


# Chargement des données


def charger_donnees(
    data_complet_path="data/data_complet.parquet",
    geojson_path="departements.geojson",
    population_path="data/data_population/population_dept.csv",
):
    """
    Charge les données nécessaires pour les cartes et graphiques.

    Paramètres
    ----------
    data_complet_path : str, optionnel
        Chemin vers le fichier parquet contenant la base complète.
    geojson_path : str, optionnel
        Chemin vers le fichier GeoJSON des départements.
    population_path : str, optionnel
        Chemin vers le CSV de population par département.

    Retour
    ------
    data_complet : pandas.DataFrame
        Base complète des licenciés (nettoyée sur `code_dep`).
    gdf_dep : geopandas.GeoDataFrame
        Géométrie des départements (doit contenir une clé `code`).
    data_pop : pandas.DataFrame
        Population par département et par année.
    """
    # Base complète : on ne conserve que les lignes avec code_dep
    data_complet = pd.read_parquet(data_complet_path)
    data_complet = data_complet.dropna(subset=["code_dep"]).copy()
    data_complet["code_dep"] = data_complet["code_dep"].astype(str)

    # Données géographiques (départements)
    gdf_dep = gpd.read_file(geojson_path)

    # Table population (référence pour les proportions)
    data_pop = pd.read_csv(population_path)

    return data_complet, gdf_dep, data_pop


# Cartes par département


def aggregation_par_an(df, year):
    """
    Agrège le nombre de licences par département pour une année donnée.

    Paramètres
    ----------
    df : pandas.DataFrame
        Base complète des licenciés.
    year : int
        Année à filtrer.

    Retour
    ------
    df_agg : pandas.DataFrame
        Table agrégée au niveau département, avec :
        - `code_dep`
        - `licences_annuelles` (somme)
    """
    df_year = df[df["annee"] == year]
    return df_year.groupby("code_dep")["licences_annuelles"].sum().reset_index()


def carte_licencies(data_complet, gdf_dep, data_pop, annee, sport="all", title=None):
    """
    Affiche une carte de la proportion de licenciés par département pour une année et
    un sport donnés. La proportion est exprimée en pourcentage.

    La population utilisée dépend de l'année :
    - si annee <= 2021 : population 2016 (hypothèse de référence)
    - sinon : population 2022

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant au moins `annee`, `sport`, `code_dep`,
        `licences_annuelles`.
    gdf_dep : geopandas.GeoDataFrame
        Géométrie des départements. Doit contenir une colonne `code`.
    data_pop : pandas.DataFrame
        Population de référence. Doit contenir `annee`, `code_dep`, `population`.
    annee : int
        Année à afficher.
    sport : str, optionnel
        Sport à filtrer. Utiliser "all" pour tous sports.
    title : str, optionnel
        Titre personnalisé de la carte.

    Retour
    ------
    Carte matplotlib représentant le pourcentage de licenciés
    par département.Les départements sans données sont indiqués
    en gris clair avec un motif hachuré.
    """
    # Filtrage des données selon le sport sélectionné
    df_filtered = (
        data_complet if sport == "all" else data_complet[data_complet["sport"] == sport]
    )

    # Agrégation des licences par département pour l'année choisie
    df_agg_lic = (
        df_filtered[df_filtered["annee"] == annee]
        .groupby("code_dep")["licences_annuelles"]
        .sum()
        .reset_index()
    )

    # Sélection de la population de référence selon l'année
    pop_ref_year = 2016 if annee <= 2021 else 2022
    df_pop = data_pop[data_pop["annee"] == pop_ref_year].copy()

    # Jointure géométrie + licences + population
    gdf_plot = gdf_dep.merge(
        df_agg_lic, left_on="code", right_on="code_dep", how="left"
    )
    gdf_plot = gdf_plot.merge(
        df_pop[["population", "code_dep"]],
        left_on="code",
        right_on="code_dep",
        how="left",
    )

    # Calcul de la proportion de licenciés (en %)
    gdf_plot["licences_annuelles_relatives"] = (
        gdf_plot["licences_annuelles"] / gdf_plot["population"]
    ) * 100

    # Tracé
    fig, ax = plt.subplots(figsize=(10, 12))  # pylint: disable=W0612
    gdf_plot.plot(
        column="licences_annuelles_relatives",
        ax=ax,
        legend=True,
        cmap="OrRd",
        edgecolor="grey",
        linewidth=0.5,
        missing_kwds={
            "color": "lightgrey",
            "edgecolor": "grey",
            "hatch": "//",
            "label": "Données manquantes",
        },
    )

    # Titre
    if sport == "all":
        titre_sport = "(tous les sports)"
    else:
        titre_sport = f"({sport})"

    ax.set_title(
        (title if title else f"Proportion de licenciés (%) {titre_sport} – {annee}"),
        fontsize=14,
    )

    # Suppression des axes
    ax.set_axis_off()

    # Affichage de la carte
    plt.show()


def carte_evolution_licencies(
    data_complet, gdf_dep, annee1, annee2, sport="all", title=None
):
    """
    Affiche une carte du taux de croissance des licenciés par département
    entre deux années pour un sport donné.

    Le taux affiché est calculé comme :
        (L2 - L1) / L1
    où L1 = licences_annuelles en annee1 et L2 = licences_annuelles en annee2.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant `annee`, `sport`, `code_dep`, `licences_annuelles`.
    gdf_dep : geopandas.GeoDataFrame
        Géométrie des départements (colonne `code`).
    annee1 : int
        Année de référence (dénominateur).
    annee2 : int
        Année de comparaison.
    sport : str, optionnel
        Sport à filtrer. Utiliser "all" pour tous sports.
    title : str, optionnel
        Titre personnalisé de la carte.

    Retour
    ------
    Carte matplotlib représentant le taux de croissance des licenciés par
    département. Les départements sans données sont indiqués en gris clair
    avec un motif hachuré.
    """
    # Filtrage des données selon le sport sélectionné
    df_filtered = (
        data_complet if sport == "all" else data_complet[data_complet["sport"] == sport]
    )

    # Agrégation du nombre de licenciés par département pour chaque année
    df1 = (
        df_filtered[df_filtered["annee"] == annee1]
        .groupby("code_dep")["licences_annuelles"]
        .sum()
        .reset_index()
    )
    df2 = (
        df_filtered[df_filtered["annee"] == annee2]
        .groupby("code_dep")["licences_annuelles"]
        .sum()
        .reset_index()
    )

    # Fusion des deux années pour calculer le taux de croissance
    df_merge = df1.merge(df2, on="code_dep", suffixes=(f"_{annee1}", f"_{annee2}"))

    # Calcul du taux de croissance
    df_merge["taux"] = (
        df_merge[f"licences_annuelles_{annee2}"]
        - df_merge[f"licences_annuelles_{annee1}"]
    ) / df_merge[f"licences_annuelles_{annee1}"]

    # Jointure avec la géométrie des départements
    gdf_plot = gdf_dep.merge(df_merge, left_on="code", right_on="code_dep", how="left")

    # Tracé
    fig, ax = plt.subplots(figsize=(10, 12))  # pylint: disable=W0612
    gdf_plot.plot(
        column="taux",
        ax=ax,
        cmap="coolwarm",
        legend=True,
        edgecolor="grey",
        linewidth=0.5,
        vmin=-0.5,
        vmax=0.5,
        missing_kwds={
            "color": "lightgrey",
            "edgecolor": "grey",
            "hatch": "//",
            "label": "Données manquantes",
        },
    )

    # Titre
    if sport == "all":
        titre_sport = "(tous les sports)"
    else:
        titre_sport = f"({sport})"

    ax.set_title(
        (title if title else f"Taux de croissance {titre_sport} – {annee1}-{annee2}"),
        fontsize=16,
    )

    # Suppression des axes
    ax.set_axis_off()

    # Affichage de la carte
    plt.show()


# Widgets pour cartes


def widgets_carte_licencies(data_complet, gdf_dep, data_pop):
    """
    Construit et affiche un widget interactif pour la carte de proportion de licenciés.

    Le widget permet de sélectionner :
    - une année
    - un sport (ou "all")

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète.
    gdf_dep : geopandas.GeoDataFrame
        Géométrie des départements.
    data_pop : pandas.DataFrame
        Population de référence.

    Retour
    ------
    None
        Affiche les widgets et la figure dans le notebook.
    """
    # Années et sports disponibles
    annees = sorted(data_complet["annee"].dropna().unique())
    sports = ["all"] + sorted(data_complet["sport"].dropna().unique())

    # Création des widgets Dropdown
    annee_widget = widgets.Dropdown(
        options=annees, description="Année :", value=annees[0]
    )
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    # Widget de sortie
    out = widgets.Output()

    # Fonction de mise à jour
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour la carte interactive lorsque l'utilisateur change l'année ou le sport.
        """
        with out:
            clear_output(wait=True)
            carte_licencies(
                data_complet,
                gdf_dep,
                data_pop,
                annee=annee_widget.value,
                sport=sport_widget.value,
            )

    # Liaison widgets - fonction
    annee_widget.observe(update, names="value")
    sport_widget.observe(update, names="value")

    # Affichage initial
    display(annee_widget, sport_widget, out)
    update()


def widgets_evolution_licencies(data_complet, gdf_dep):
    """
    Construit et affiche un widget interactif pour la carte d'évolution des licenciés.

    Le widget permet de sélectionner :
    - une année 1 (référence)
    - une année 2 (comparaison)
    - un sport (ou "all")

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète.
    gdf_dep : geopandas.GeoDataFrame
        Géométrie des départements.

    Retour
    ------
    None
        Affiche les widgets et la figure dans le notebook.
    """
    # Années et sports disponibles
    annees = sorted(data_complet["annee"].dropna().unique())
    sports = ["all"] + sorted(data_complet["sport"].dropna().unique())

    # Création des widgets Dropdown
    annee1_widget = widgets.Dropdown(
        options=annees, description="Année 1 :", value=annees[0]
    )
    annee2_widget = widgets.Dropdown(
        options=annees,
        description="Année 2 :",
        value=annees[1] if len(annees) > 1 else annees[0],
    )
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    # Widget de sortie
    out = widgets.Output()

    # Fonction de mise à jour
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour la carte interactive lorsque l'utilisateur change l'année ou le sport.
        """
        with out:
            clear_output(wait=True)
            carte_evolution_licencies(
                data_complet,
                gdf_dep,
                annee1=annee1_widget.value,
                annee2=annee2_widget.value,
                sport=sport_widget.value,
            )

    # Liaison widgets - fonction
    annee1_widget.observe(update, names="value")
    annee2_widget.observe(update, names="value")
    sport_widget.observe(update, names="value")

    # Affichage initial
    display(annee1_widget, annee2_widget, sport_widget, out)
    update()


# Graphiques par âge


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
    fig.show()


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
    fig.show()


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
    fig.show()


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
    fig.show()


# Widgets pour les graphiques par âge


def widgets_evolution_licencies_age(data_complet):
    """
    Widget interactif pour afficher l'évolution des licenciés par âge exact.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant la colonne `age`.

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Tri des les âges numériques, on conserve "NR - Non réparti" à part
    ages_numeric = sorted(
        [a for a in data_complet["age"].dropna().unique() if a != "NR - Non réparti"],
        key=int,
    )
    # Ajout de "NR - Non réparti" à la fin si présent
    ages = ages_numeric + (
        ["NR - Non réparti"]
        if "NR - Non réparti" in data_complet["age"].unique()
        else []
    )

    # Création du widget Dropdown
    age_widget = widgets.Dropdown(
        options=["all"] + ages, description="Age :", value="all"
    )

    # Widget de sortie
    out = widgets.Output()

    # Fonction de mise à jour du graphique
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour le graphique interactif lorsque l'utilisateur change l'âge sélectionné.
        """
        with out:
            clear_output(wait=True)
            evolution_licencies_age(data_complet, age=age_widget.value)

    # Liaison du widget à la fonction de mise à jour
    age_widget.observe(update, names="value")

    # Affichage initial
    display(age_widget, out)
    update()


def widgets_evolution_licences_tranches_fines_age(data_complet):
    """
    Widget interactif pour afficher l'évolution des licenciés par tranche d'âge fine.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant la colonne `tranche_age`.

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Récupération et tri des tranches d'âge uniques
    tranches = sorted(data_complet["tranche_age"].dropna().unique())

    # Création du widget Dropdown
    tranche_widget = widgets.Dropdown(
        options=["all"] + tranches, description="Tranche :", value="all"
    )

    # Widget de sortie
    out = widgets.Output()

    # Fonction de mise à jour du graphique
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour le graphique interactif lorsque l'utilisateur change la tranche sélectionnée.
        """
        with out:
            clear_output(wait=True)
            evolution_licences_tranches_fines_age(
                data_complet, tranche=tranche_widget.value
            )

    # Liaison du widget à la fonction de mise à jour
    tranche_widget.observe(update, names="value")

    # Affichage initial
    display(tranche_widget, out)
    update()


def widgets_repartition_grandes_tranches_age_par_sport(data_complet):
    """
    Widget interactif pour la répartition des licenciés par grande tranche d'âge.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant `annee` et `grande_tranche_age`.

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Récupération des années uniques et triées
    annees = sorted(data_complet["annee"].dropna().unique())

    # Création du widget Dropdown
    annees_widget = widgets.Dropdown(
        options=["all"] + list(annees), description="Année :", value="all"
    )

    # Widget de sortie
    out = widgets.Output()

    # Fonction de mise à jour du graphique
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour le graphique interactif lorsque l'utilisateur change l'année sélectionnée.
        """
        with out:
            clear_output(wait=True)
            repartition_grandes_tranches_age_par_sport(
                data_complet, annee=annees_widget.value
            )

    # Liaison du widget à la fonction de mise à jour
    annees_widget.observe(update, names="value")

    # Affichage initial
    display(annees_widget, out)
    update()


def widgets_repartition_fines_tranches_age_par_sport(data_complet):
    """
    Widget interactif pour la répartition des licenciés par tranche d'âge fine.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant `annee` et `tranche_age`.

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Récupération des années uniques et triées
    annees = sorted(data_complet["annee"].dropna().unique())

    # Création du widget Dropdown
    annees_widget = widgets.Dropdown(
        options=["all"] + list(annees), description="Année :", value="all"
    )

    # Widget de sortie pour afficher le graphique
    out = widgets.Output()

    # Fonction de mise à jour du graphique
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour le graphique interactif lorsque l'utilisateur change l'année sélectionnée.
        """
        with out:
            clear_output(wait=True)
            repartition_fines_tranches_age_par_sport(
                data_complet, annee=annees_widget.value
            )

    # Liaison du widget à la fonction de mise à jour
    annees_widget.observe(update, names="value")

    # Affichage initial
    display(annees_widget, out)
    update()
