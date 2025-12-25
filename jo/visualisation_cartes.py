"""
Cartes géographiques (geopandas + matplotlib).
Ces fonctions affichent avec plt.show().
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap


def carte_licencies(data_complet, gdf_dep, data_pop, annee, sport="all"):
    """
    Affiche une carte de la proportion de licenciés par département pour une année et
    un sport donnés. La proportion est exprimée en pourcentage.

    Paramètres
    ----------
    data_complet : pd.DataFrame
        Doit contenir au moins ['annee','code_dep','licences_annuelles','sport'].
    gdf_dep : GeoDataFrame
        Doit contenir 'code_dep' + 'geometry'.
    data_pop : pd.DataFrame
        Doit contenir ['annee','code_dep','population'].
    annee : int
    sport : str
        "all" ou nom exact du sport.

    Affiche
    -------
    Carte matplotlib.
    """
    df = data_complet.dropna(subset=["code_dep"]).copy()

    # Filtrage des données selon le sport
    if sport != "all":
        df = df[df["sport"] == sport]

    # Agrégation des licences par département pour l'année choisie
    lic = (
        df[df["annee"] == annee]
        .groupby("code_dep", as_index=False)["licences_annuelles"]
        .sum()
    )

    # Sélection de la population de référence selon l'année
    pop = data_pop.copy()
    pop["annee"] = pop["annee"].astype(int)
    pop = pop[pop["annee"] <= int(annee)]
    pop_ref_year = pop["annee"].max()
    pop = pop[pop["annee"] == pop_ref_year][["code_dep", "population"]]

    # Jointure géométrie + licences + population
    merged = gdf_dep.merge(lic, on="code_dep", how="left").merge(
        pop, on="code_dep", how="left"
    )

    # Calcul de la proportion de licenciés (en %)
    merged["taux_licencies"] = 100 * merged["licences_annuelles"] / merged["population"]

    MATTER_COLORS = [
        "#fff2c6",
        "#f6c48b",
        "#ee9b6a",
        "#d65a5a",
        "#8b2d5c",
        "#4b1d4a",
    ]

    _CMAP_LICENCIES = LinearSegmentedColormap.from_list(
        "matter_manual", MATTER_COLORS, N=256
    )
    # Titre
    titre_sport = "tous les sports" if sport == "all" else sport

    # Tracé
    ax = merged.plot(
        column="taux_licencies",
        legend=True,
        figsize=(10, 8),
        cmap=_CMAP_LICENCIES,
        edgecolor="grey",
        linewidth=0.5,
        missing_kwds={
            "color": "lightgrey",
            "edgecolor": "grey",
            "label": "Données manquantes",
        },
    )

    # Titre
    ax.set_title(
        f"Part des licenciés dans la population (%) – {titre_sport} – {annee} (population de {pop_ref_year})"
    )

    # Suppression des axes
    ax.axis("off")

    # Affichage de la carte
    plt.show()


def carte_evolution_licencies(data_complet, gdf_dep, annee1, annee2, sport="all"):
    """
    Affiche une carte du taux de croissance des licenciés par département
    entre deux années pour un sport donné.

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
    df = data_complet.dropna(subset=["code_dep"]).copy()

    # Filtrage des données selon le sport sélectionné
    if sport != "all":
        df = df[df["sport"] == sport]

    # Agrégation du nombre de licenciés par département pour chaque année
    d1 = (
        df[df["annee"] == annee1]
        .groupby("code_dep", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "v1"})
    )
    d2 = (
        df[df["annee"] == annee2]
        .groupby("code_dep", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "v2"})
    )

    # Fusion des deux années et avec la géométrie des départements
    merged = gdf_dep.merge(d1, on="code_dep", how="left").merge(
        d2, on="code_dep", how="left"
    )

    # Calcul du taux de croissance
    merged["evolution"] = (merged["v2"] - merged["v1"]) / merged["v1"]

    # Tracé
    ax = merged.plot(
        column="evolution",
        legend=True,
        figsize=(10, 8),
        edgecolor="grey",
        linewidth=0.5,
        cmap="coolwarm",
        missing_kwds={
            "color": "lightgrey",
            "edgecolor": "grey",
            "label": "Données manquantes",
        },
    )

    # Titre
    if sport == "all":
        titre_sport = "(tous les sports)"
    else:
        titre_sport = f"({sport})"

    ax.set_title(f"Taux de croissance {titre_sport} – {annee1}-{annee2}")

    # Suppression des axes
    ax.axis("off")

    # Affichage de la carte
    plt.show()
