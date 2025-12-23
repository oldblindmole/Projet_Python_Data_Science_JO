"""
Cartes géographiques (geopandas + matplotlib).
Ces fonctions affichent avec plt.show().
"""

import matplotlib.pyplot as plt
#import geopandas as gpd


def carte_licencies(df, gdf_dep, annee):
    """TODO"""
    data = df[df["annee"] == annee]
    merged = gdf_dep.merge(data, on="code_dep", how="left")

    ax = merged.plot(
        column="licences_annuelles",
        legend=True,
        figsize=(10, 8),
        missing_kwds={"color": "lightgrey"},
    )
    ax.set_title(f"Licenciés par département – {annee}")
    ax.axis("off")
    plt.show()


def carte_evolution_licencies(df, gdf_dep, annee1, annee2):
    """TODO"""
    d1 = df[df["annee"] == annee1][["code_dep", "licences_annuelles"]].rename(
        columns={"licences_annuelles": "v1"}
    )
    d2 = df[df["annee"] == annee2][["code_dep", "licences_annuelles"]].rename(
        columns={"licences_annuelles": "v2"}
    )

    merged = gdf_dep.merge(d1, on="code_dep", how="left").merge(d2, on="code_dep", how="left")
    merged["evolution"] = merged["v2"] - merged["v1"]

    ax = merged.plot(
        column="evolution",
        legend=True,
        figsize=(10, 8),
        cmap="coolwarm",
        missing_kwds={"color": "lightgrey"},
    )
    ax.set_title(f"Évolution des licenciés {annee1} → {annee2}")
    ax.axis("off")
    plt.show()
