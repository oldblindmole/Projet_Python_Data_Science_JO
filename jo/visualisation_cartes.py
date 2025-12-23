"""
Cartes géographiques (geopandas + matplotlib).
Ces fonctions affichent avec plt.show().
"""

import matplotlib.pyplot as plt
#import geopandas as gpd


def carte_licencies(data_complet, gdf_dep, data_pop, annee, sport="all"):
    """
    Carte : proportion de licenciés (licences / population) par département.

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
    df = data_complet.copy()
    if sport != "all":
        df = df[df["sport"] == sport]

    lic = (
        df[df["annee"] == annee]
        .groupby("code_dep", as_index=False)["licences_annuelles"]
        .sum()
    )

    # Population : on prend l'année si dispo, sinon la plus récente <= annee
    pop = data_pop.copy()
    pop["annee"] = pop["annee"].astype(int)
    pop = pop[pop["annee"] <= int(annee)]
    if pop.empty:
        raise ValueError("data_pop ne contient aucune année <= annee demandée.")
    pop_ref_year = pop["annee"].max()
    pop = pop[pop["annee"] == pop_ref_year][["code_dep", "population"]]

    merged = gdf_dep.merge(lic, on="code_dep", how="left").merge(pop, on="code_dep", how="left")
    merged["taux_licencies"] = 100 * merged["licences_annuelles"] / merged["population"]

    ax = merged.plot(
        column="taux_licencies",
        legend=True,
        figsize=(10, 8),
        missing_kwds={"color": "lightgrey"},
    )
    ax.set_title(f"Licenciés / population (%) – {sport} – {annee} (pop {pop_ref_year})")
    ax.axis("off")
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
    df = data_complet.copy()
    if sport != "all":
        df = df[df["sport"] == sport]

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

    merged = gdf_dep.merge(d1, on="code_dep", how="left").merge(d2, on="code_dep", how="left")
    merged["evolution"] = (merged["v2"] - merged["v1"]) / merged["v1"]

    ax = merged.plot(
        column="evolution",
        legend=True,
        figsize=(10, 8),
        cmap="coolwarm",
        missing_kwds={"color": "lightgrey"},
    )
    ax.set_title(f"Évolution relative des licenciés – {sport} – {annee1} → {annee2}")
    ax.axis("off")
    plt.show()
