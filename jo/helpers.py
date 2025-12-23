"""
Fonctions utilitaires pour la préparation des données.
Aucune visualisation ni widget ici.
"""

import re
import pandas as pd
import numpy as np


def filtrer_sport(df: pd.DataFrame, sport: str, sport_col: str = "sport") -> pd.DataFrame:
    """
    Filtre un DataFrame par sport.

    Paramètres
    ----------
    df : pd.DataFrame
    sport : str
        "all" ou nom exact du sport.
    sport_col : str

    Retour
    ------
    pd.DataFrame
    Retourne df filtré sur un sport, ou df inchangé si sport == 'all'.
    """
    if sport == "all":
        return df
    return df[df[sport_col] == sport]


def agregation_licences_par_annee(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège le nombre de licences par année.
    Colonnes requises : ['annee', 'licences_annuelles']

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
    return (
        df.groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .sort_values("annee")
    )


def pivot_licences_tranche_age(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot licences : lignes = tranche_age, colonnes = annee.
    """
    pivot = df.pivot_table(
        index="tranche_age",
        columns="annee",
        values="licences_annuelles",
        aggfunc="sum",
        fill_value=0,
    )

    def _lower_bound(x):
        m = re.search(r"\d+", str(x))
        return int(m.group()) if m else 10**9

    pivot = pivot.loc[sorted(pivot.index, key=_lower_bound)]
    pivot = pivot.sort_index(axis=1)
    return pivot


def part_sous_population(
    df: pd.DataFrame,
    condition: pd.Series,
) -> pd.DataFrame:
    """
    Calcule la part d'une sous-population par année.

    Paramètres
    ----------
    df : pd.DataFrame
        Colonnes requises : ['annee', 'licences_annuelles']
    condition : pd.Series (bool)
        Condition de sélection de la sous-population.

    Retour
    ------
    pd.DataFrame avec colonnes ['annee', 'part']
    """
    total = (
        df.groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "total"})
    )

    sub = (
        df[condition]
        .groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "sub"})
    )

    merged = total.merge(sub, on="annee", how="left").fillna({"sub": 0})
    merged["part"] = np.where(
        merged["total"] > 0, merged["sub"] / merged["total"], np.nan
    )

    return merged.sort_values("annee")
