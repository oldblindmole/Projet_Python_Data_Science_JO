"""
Module de visualisation interactive des données de licenciés.

Ce module est conçu pour être importé et utilisé depuis le notebook
principal (main.ipynb).
"""

import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import ipywidgets as widgets
from IPython.display import display, clear_output


# helpers

def _filtrer_sport(df: pd.DataFrame, sport: str):
    """Retourne df filtré sur un sport, ou df inchangé si sport == 'all'."""
    return df if sport == "all" else df[df["sport"] == sport]


def _run_widget(update_fn, controls):
    """
    Affiche des contrôles + un Output, et relance update_fn() quand un contrôle change.

    Paramètres
    ----------
    update_fn : callable
        Fonction sans argument qui trace/affiche (elle lit les valeurs via closure).
    controls : list
        Liste de widgets (Dropdown, Slider...) à observer.

    Retour
    ------
    out : widgets.Output
        Output widget dans lequel sont affichés les graphiques.
    """
    out = widgets.Output()

    def _wrapped_update(change=None):  # pylint: disable=unused-argument
        with out:
            clear_output(wait=True)
            res = update_fn()
            if res is not None:
                display(res)


    for c in controls:
        c.observe(_wrapped_update, names="value")

    display(*controls, out)
    _wrapped_update()



# II. Chargement des données


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


# II.A. Medailles et licenciés


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
    # --- Licences (courbe) ---
    d = df if sport == "all" else df[df["sport"] == sport]
    lic = (
        d.groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .sort_values("annee")
    )

    years = lic["annee"].tolist()

    jo_col_years = [2016, 2020, 2024]
    jo_display_year = {2016: 2016, 2020: 2021, 2024: 2024}
    medal_types = ("or", "argent", "bronze")
    medal_colors = {"or": "#F2C300", "argent": "#B0B0B0", "bronze": "#8C6239"}

    # --- Médailles (barres) ---
    # On initialise à 0 sur toutes les années de la courbe licences
    medals_by_year = {y: {m: 0 for m in medal_types} for y in years}

    # On ne trace des médailles que pour un sport spécifique (pas "all")
    if sport != "all":
        # Récupère UNE ligne par sport pour les colonnes médailles
        cols_needed = ["sport"] + [f"{y}_{m}" for y in jo_col_years for m in medal_types]
        cols_needed = [c for c in cols_needed if c in df.columns]  # sécurité

        med = (
            df[cols_needed]
            .drop_duplicates(subset=["sport"])
            .groupby("sport", as_index=False)
            .first()
        )

        row = med[med["sport"] == sport]
        if not row.empty:
            row = row.iloc[0]

            # Remplit uniquement les années JO affichées (2016, 2021, 2024)
            for y_col in jo_col_years:
                y_display = jo_display_year[y_col]
                if y_display in medals_by_year:
                    for m in medal_types:
                        col = f"{y_col}_{m}"
                        val = 0 if (col not in med.columns or pd.isna(row[col])) else int(row[col])
                        medals_by_year[y_display][m] = val

    med_or = [medals_by_year[y]["or"] for y in years]
    med_arg = [medals_by_year[y]["argent"] for y in years]
    med_bro = [medals_by_year[y]["bronze"] for y in years]

    # --- Figure (double axe) ---
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Courbe licences (axe gauche)
    fig.add_trace(
        go.Scatter(
            x=years,
            y=lic["licences_annuelles"],
            mode="lines+markers",
            name="Licenciés",
            hovertemplate="Année: %{x}<br>Licenciés: %{y:,}<extra></extra>",
        ),
        secondary_y=False,
    )

    # Barres médailles (axe droit) empilées
    fig.add_trace(
        go.Bar(x=years, y=med_or, name="Or", marker_color=medal_colors["or"],
               hovertemplate="Année: %{x}<br>Or: %{y}<extra></extra>"),
        secondary_y=True,
    )
    fig.add_trace(
        go.Bar(x=years, y=med_arg, name="Argent", marker_color=medal_colors["argent"],
               hovertemplate="Année: %{x}<br>Argent: %{y}<extra></extra>"),
        secondary_y=True,
    )
    fig.add_trace(
        go.Bar(x=years, y=med_bro, name="Bronze", marker_color=medal_colors["bronze"],
               hovertemplate="Année: %{x}<br>Bronze: %{y}<extra></extra>"),
        secondary_y=True,
    )

    titre = "Tous sports" if sport == "all" else sport
    fig.update_layout(
        title=f"Évolution des licenciés et médailles – {titre}",
        width=1100,
        height=600,
        barmode="stack",
        hovermode="x unified",
        legend_title_text="Légende",
    )
    fig.update_xaxes(title_text="Année")
    fig.update_yaxes(title_text="Nombre de licenciés", secondary_y=False)
    fig.update_yaxes(title_text="Nombre de médailles", secondary_y=True)

    return fig


def widget_graphique_licences_et_medailles(data_complet: pd.DataFrame):
    """TODO"""
    sports = ["all"] + sorted(data_complet["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    def update():
        fig = graphique_licences_et_medailles(data_complet, sport=sport_widget.value)
        display(fig)

    _run_widget(update, [sport_widget])


def classement_sports_medailles(data_complet, annee="all"):
    """
    Construit un classement des sports selon le nombre de médailles remportées
    aux Jeux Olympiques.

    Deux modes sont possibles :
    - annee = "all" : cumul des JO 2016, 2020 et 2024
    - annee = 2016, 2020 ou 2024 : classement pour une édition donnée

    Les médailles sont pondérées de la façon suivante :
    - Or = 3 points
    - Argent = 2 points
    - Bronze = 1 point

    Les sports n'ayant remporté aucune médaille sur la période considérée
    sont exclus du classement.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant la colonne 'sport' et les colonnes de médailles.
    annee : int ou str, optionnel (par défaut = "all")
        Année des Jeux Olympiques à considérer (2016, 2020, 2024)
        ou "all" pour un cumul sur toutes les éditions.

    Retour
    ------
    df_classement : pandas.DataFrame
        Table de classement des sports, triée par score pondéré décroissant.
    """
    # Années JO disponibles
    annees_disponibles = [2016, 2020, 2024]

    if annee == "all":
        annees = annees_disponibles
    else:
        if annee not in annees_disponibles:
            raise ValueError(f"annee doit être dans {annees_disponibles} ou 'all'")
        annees = [annee]

    # Colonnes de médailles à utiliser
    cols_medailles = ["sport"]
    for a in annees:
        cols_medailles += [f"{a}_or", f"{a}_argent", f"{a}_bronze"]

    # Une ligne par sport (les médailles sont répétées dans la base)
    df = (
        data_complet[cols_medailles]
        .drop_duplicates(subset=["sport"])
        .groupby("sport", as_index=False)
        .first()
    )

    # Initialisation des totaux
    df["total_or"] = 0
    df["total_argent"] = 0
    df["total_bronze"] = 0

    # Cumul des médailles sur les années sélectionnées
    for a in annees:
        df["total_or"] += df[f"{a}_or"]
        df["total_argent"] += df[f"{a}_argent"]
        df["total_bronze"] += df[f"{a}_bronze"]

    # Total simple
    df["total_medailles"] = df["total_or"] + df["total_argent"] + df["total_bronze"]

    # Score pondéré
    df["score_pondere"] = (
        3 * df["total_or"] + 2 * df["total_argent"] + 1 * df["total_bronze"]
    )

    # Exclusion des sports sans médailles et classement
    df_classement = (
        df[df["total_medailles"] > 0]
        .sort_values("score_pondere", ascending=False)
        .reset_index(drop=True)
    )

    # Colonnes finales
    df_classement = df_classement[
        [
            "sport",
            "total_medailles",
            "total_or",
            "total_argent",
            "total_bronze",
            "score_pondere",
        ]
    ]

    return df_classement


def croissance_licencies_post_jo(data_complet, annee_jo, delta=2):
    """
    Calcule le taux de croissance du nombre de licenciés sportifs entre l'année
    des Jeux Olympiques (t) et t + delta, en se restreignant aux sports ayant
    remporté au moins une médaille lors de cette même édition.

    Cas particulier :
    - Les Jeux Olympiques de 2020 s'étant tenus en 2021, l'année 2021 est utilisée
      comme année de référence pour les licenciés, tandis que les médailles
      restent rattachées à l'édition JO 2020.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète des licenciés. Doit contenir au minimum :
        - 'sport', 'annee', 'licences_annuelles'
        - les colonnes de médailles : '{annee}_or', '{annee}_argent', '{annee}_bronze'
    annee_jo : int
        Année des Jeux Olympiques (ex : 2016, 2020, 2024).
    delta : int, optionnel (par défaut = 2)
        Horizon temporel (en années) pour mesurer la croissance post-JO.

    Retour
    ------
    df_croissance : pandas.DataFrame
        Table classée par taux de croissance décroissant, contenant :
        - sport
        - annee_jo
        - annee_licences_t
        - licences_t
        - licences_t_plus_2
        - taux_croissance (en %)
    """
    # Agrégation préalable : licenciés par sport et par année
    lic_sport_annee = data_complet.groupby(["sport", "annee"], as_index=False)[
        "licences_annuelles"
    ].sum()

    # Année de référence pour les licenciés (cas particulier JO 2020)
    annee_lic = 2021 if annee_jo == 2020 else annee_jo
    annee_lic_plus_delta = annee_lic + delta

    rows = []

    # Identification des sports ayant remporté au moins une médaille l'année JO
    cols_jo = [f"{annee_jo}_or", f"{annee_jo}_argent", f"{annee_jo}_bronze"]
    sports_medailes = (
        data_complet[data_complet[cols_jo].fillna(0).sum(axis=1) > 0]["sport"]
        .dropna()
        .unique()
    )

    # Calcul du taux de croissance pour chaque sport médaillé
    for sport in sports_medailes:
        df_s = lic_sport_annee[lic_sport_annee["sport"] == sport]

        # On vérifie la présence des deux années nécessaires
        if (df_s["annee"] == annee_lic).any() and (
            df_s["annee"] == annee_lic_plus_delta
        ).any():
            L_t = float(
                df_s.loc[df_s["annee"] == annee_lic, "licences_annuelles"].iloc[0]
            )
            L_t_delta = float(
                df_s.loc[
                    df_s["annee"] == annee_lic_plus_delta, "licences_annuelles"
                ].iloc[0]
            )

            # Sécurité : éviter une division par zéro
            if L_t > 0:
                rows.append(
                    {
                        "sport": sport,
                        "annee_jo": annee_jo,
                        "annee_licences_t": annee_lic,
                        "licences_t": L_t,
                        "licences_t_plus_2": L_t_delta,
                        "taux_croissance": 100 * (L_t_delta - L_t) / L_t,
                    }
                )

    # Construction du DataFrame final
    df_croissance = pd.DataFrame(
        rows,
        columns=[
            "sport",
            "annee_jo",
            "annee_licences_t",
            "licences_t",
            "licences_t_plus_2",
            "taux_croissance",
        ],
    )

    # Classement par taux de croissance décroissant
    if not df_croissance.empty:
        df_croissance = df_croissance.sort_values(
            "taux_croissance", ascending=False
        ).reset_index(drop=True)

    return df_croissance


def licences_par_annee(df_lic, sport_code="all", sport_col="code_sport"):
    """
    Affiche un graphique interactif des licences annuelles.

    Paramètres
    ----------
    df_lic : pd.DataFrame
        DataFrame contenant au moins les colonnes ['annee', 'licences_annuelles', sport_col].
    sport_code : str ou None, optionnel
        Filtre pour un code de sport spécifique. Si None, affiche tous les sports.
    sport_col : str
        Nom de la colonne contenant le code sport (par défaut 'code_sport').
    output_widget : ipywidgets.Output ou None
        Si fourni, le graphique sera affiché dans ce widget pour
        permettre une mise à jour interactive.

    Output
    ------
    Graphique interactif Plotly des licences annuelles.
    """

    df = df_lic.copy()

    # Filtrage par sport si demandé
    if sport_code == "all":
        df = df.copy()
        titre_sport = "tous les sports"
    else:
        df = df[df[sport_col] == sport_code]
        noms_sports = df["sport"].dropna().unique()
        if len(noms_sports) == 1:
            titre_sport = noms_sports[0]
        else:
            titre_sport = ", ".join(noms_sports)

    # Agrégation des licences par année
    data = (
        df.groupby("annee")["licences_annuelles"]
        .sum()
        .reset_index()
        .sort_values("annee")
    )

    # Calcul de la variation annuelle et du taux d'évolution
    if len(data) >= 2:
        data["variation"] = data["licences_annuelles"].diff()
        data["taux_evolution_%"] = data["licences_annuelles"].pct_change() * 100
    else:
        data["variation"] = np.nan
        data["taux_evolution_%"] = np.nan

    # Tracé
    fig = px.line(
        data,
        x="annee",
        y="licences_annuelles",
        title=f"Licences annuelles - {titre_sport}",
        markers=True,
        labels={"annee": "Année", "licences_annuelles": "Licences annuelles"},
    )

    fig.update_layout(xaxis=dict(dtick=1))
    return fig


def widget_licences_par_sport(
    df, sport_name_col="sport", sport_code_col="code_sport"
):
    """
    Crée un widget interactif (Dropdown) permettant de sélectionner un sport
    et d'afficher le graphique correspondant via la fonction `plot_licences_par_annee`.

    Paramètres
    ----------
    df : pandas.DataFrame
        DataFrame contenant au minimum les colonnes :
        - sport_name_col (par défaut 'sport')
        - sport_code_col (par défaut 'code_sport')
    sport_name_col : str, optionnel
        Nom de la colonne contenant le libellé du sport (par défaut 'sport').
    sport_code_col : str, optionnel
        Nom de la colonne contenant le code du sport (par défaut 'code_sport').

    Retour
    ------
    tuple (sports_widget, out)
        - sports_widget : widget Dropdown (utile si tu veux le manipuler ensuite)
        - out : widget Output (utile si tu veux contrôler l'affichage)
    """
    # Préparation des options : "all" + liste des sports
    sports_names = sorted(df[sport_name_col].dropna().unique())
    options = ["all"] + list(sports_names)

    # Création des widgets
    sports_widget = widgets.Dropdown(
        options=options,
        description="Sports :",
        value="all",
    )
    out = widgets.Output()


    def update_graph(change=None):  # pylint: disable=W0613
        """
        Met à jour le graphique lorsque l'utilisateur change le sport sélectionné.
        """
        with out:
            clear_output(wait=True)

            selected_sport = sports_widget.value

            # Cas 1 : tous sports
            if selected_sport == "all":
                licences_par_annee(df, sport_code="all", sport_col=sport_code_col)
                return

            # Cas 2 : un sport spécifique -> conversion nom -> code(s)
            codes = (
                df.loc[df[sport_name_col] == selected_sport, sport_code_col]
                .dropna()
                .unique()
            )

            if len(codes) == 0:
                print(f"Aucun code sport trouvé pour : {selected_sport}")
                return

            sport_code = codes[0] if len(codes) == 1 else list(codes)

            licences_par_annee(df, sport_code=sport_code, sport_col=sport_code_col)

    _run_widget(update_graph, [sports_widget])


# Evolution des licenciés selon le nombre de medailles
def _table_licences_par_sport_annee(df: pd.DataFrame, sport: str) -> pd.DataFrame:
    d = df if sport == "all" else df[df["sport"] == sport]
    lic = (
        d.groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .sort_values("annee")
    )
    return lic


def _medailles_par_sport(df: pd.DataFrame, sport: str) -> dict:
    """
    Retourne un dict {2016: {'or':x,'argent':y,'bronze':z}, 2020:..., 2024:...}
    """
    cols_needed = [
        "sport",
        "2016_or",
        "2016_argent",
        "2016_bronze",
        "2020_or",
        "2020_argent",
        "2020_bronze",
        "2024_or",
        "2024_argent",
        "2024_bronze",
    ]
    cols_needed = [c for c in cols_needed if c in df.columns]
    med = df[cols_needed].drop_duplicates(subset=["sport"]).copy()
    med = med.groupby("sport", as_index=False).first()

    row = med[med["sport"] == sport]
    if row.empty:
        return {
            2016: {"or": 0, "argent": 0, "bronze": 0},
            2021: {"or": 0, "argent": 0, "bronze": 0},
            2024: {"or": 0, "argent": 0, "bronze": 0},
        }

    row = row.iloc[0]

    def get(col):
        return 0 if (col not in med.columns or pd.isna(row[col])) else int(row[col])

    return {
        2016: {
            "or": get("2016_or"),
            "argent": get("2016_argent"),
            "bronze": get("2016_bronze"),
        },
        2021: {
            "or": get("2020_or"),
            "argent": get("2020_argent"),
            "bronze": get("2020_bronze"),
        },
        2024: {
            "or": get("2024_or"),
            "argent": get("2024_argent"),
            "bronze": get("2024_bronze"),
        },
    }


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


def carte_licencies(data_complet, gdf_dep, data_pop, annee, sport="all"):
    """
    Affiche une carte de la proportion de licenciés par département pour une année et
    un sport donnés. La proportion est exprimée en pourcentage.

    La population utilisée dépend de l'année :
    - si annee <= 2023 : population de l'année (hypothèse de référence)
    - sinon : population 2023

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

    Retour
    ------
    Carte matplotlib représentant le pourcentage de licenciés
    par département.Les départements sans données sont indiqués
    en gris clair avec un motif hachuré.
    """
    # Filtrage des données selon le sport sélectionné
    df_filtered = _filtrer_sport(data_complet, sport)

    # Agrégation des licences par département pour l'année choisie
    df_agg_lic = (
        df_filtered[df_filtered["annee"] == annee]
        .groupby("code_dep")["licences_annuelles"]
        .sum()
        .reset_index()
    )

    # Sélection de la population de référence selon l'année
    pop_ref_year = annee if annee <= 2023 else 2023
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
        f"Proportion de licenciés (%) {titre_sport} – {annee}",
        fontsize=14,
    )

    # Suppression des axes
    ax.set_axis_off()

    # Affichage de la carte
    plt.show()


def carte_evolution_licencies(data_complet, gdf_dep, annee1, annee2, sport="all"):
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

    Retour
    ------
    Carte matplotlib représentant le taux de croissance des licenciés par
    département. Les départements sans données sont indiqués en gris clair
    avec un motif hachuré.
    """
    # Filtrage des données selon le sport sélectionné
    df_filtered = _filtrer_sport(data_complet, sport)


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
        f"Taux de croissance {titre_sport} – {annee1}-{annee2}",
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

    _run_widget(update, [annee_widget, sport_widget])


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

    _run_widget(update, [annee1_widget, annee2_widget, sport_widget])


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

    _run_widget(update, [age_widget])


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

    _run_widget(update, [tranche_widget])


def widgets_evolution_licences_tranches_grande_age(data_complet):
    """
    Widget interactif pour afficher l'évolution des licenciés par grande tranche d'âge.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant la colonne `grande_tranche_age`.

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Récupération et tri des tranches d'âge uniques
    tranches = sorted(data_complet["grande_tranche_age"].dropna().unique())

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
            evolution_licences_tranches_grandes_age(
                data_complet, tranche=tranche_widget.value
            )

    _run_widget(update, [tranche_widget])


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

    _run_widget(update, [annees_widget])


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

    _run_widget(update, [annees_widget])
