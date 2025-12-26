"""
Fonctions utilitaires pour la préparation des données.
Aucune visualisation ni widget ici.
"""

import re
import pandas as pd
import numpy as np


def filtrer_sport(df, sport, sport_col = "sport"):
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


def agregation_licences_par_annee(df):
    """
    Agrège le nombre de licences par année.

    Paramètres
    ----------
    df : pd.DataFrame

    Retour
    ------
    pd.DataFrame
        Colonnes : ['annee', 'licences_annuelles'], triées par année.
    """

    return (
        df.groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .sort_values("annee")
    )


def pivot_licences_tranche_age(df):
    """
    Construit une table pivot des licences sportives par tranche d'âge et par année.

    Paramètres
    ----------
    df : pandas.DataFrame

    Retour
    ------
    pivot : pandas.DataFrame
    """

    # Création de la table pivot : agrégation des licences par tranche d'âge et année
    pivot = df.pivot_table(
        index="tranche_age",
        columns="annee",
        values="licences_annuelles",
        aggfunc="sum",
        fill_value=0,
    )

    # Fonction pour extraire la borne inférieure numérique d'une tranche d'âge
    def _lower_bound(x):
        m = re.search(r"\d+", str(x))
        return int(m.group()) if m else 10**9

    # Tri des tranches d'âge selon leur borne inférieure
    pivot = pivot.loc[sorted(pivot.index, key=_lower_bound)]

    # Tri chronologique des années
    pivot = pivot.sort_index(axis=1)

    return pivot


def part_sous_population(df,condition,):
    """
    Calcule la part d'une sous-population par année dans le total
    des licenciés.

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

    # Total des licences par année (population complète)
    total = (
        df.groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "total"})
    )

    # Total des licences par année pour la sous-population
    sub = (
        df[condition]
        .groupby("annee", as_index=False)["licences_annuelles"]
        .sum()
        .rename(columns={"licences_annuelles": "sub"})
    )

    # Fusion des totaux et gestion des années sans sous-population
    merged = total.merge(sub, on="annee", how="left").fillna({"sub": 0})

    # Calcul de la part
    merged["part"] = np.where(
        merged["total"] > 0, merged["sub"] / merged["total"], np.nan
    )

    return merged.sort_values("annee")


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
        for m, col_total in [
            ("or", "total_or"),
            ("argent", "total_argent"),
            ("bronze", "total_bronze"),
        ]:
            col = f"{a}_{m}"
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            df[col_total] += df[col].astype(int)

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


def croissance_licencies_post_jo(data_complet, annee_jo, delta):
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
    # Agrégation des licenciés par sport et par année
    lic_sport_annee = data_complet.groupby(["sport", "annee"], as_index=False)[
        "licences_annuelles"
    ].sum()

    # Année de référence pour les licenciés
    annee_lic_plus_delta = annee_jo + delta

    # Année de référence pour les médailles 
    if annee_jo == 

    rows = []

    # Identification des sports ayant remporté au moins une médaille l'année JO
    cols_jo = [f"{annee_jo}_or", f"{annee_jo}_argent", f"{annee_jo}_bronze"]
    for c in cols_jo:
        if c not in data_complet.columns:
            data_complet[c] = 0
        data_complet[c] = pd.to_numeric(data_complet[c], errors="coerce").fillna(0)

    sports_medailes = (
        data_complet[data_complet[cols_jo].sum(axis=1) > 0]["sport"].dropna().unique()
    )

    # Calcul du taux de croissance pour chaque sport médaillé
    for sport in sports_medailes:
        df_s = lic_sport_annee[lic_sport_annee["sport"] == sport]

        # Vérification de la présence des deux années nécessaires
        if (df_s["annee"] == annee_jo).any() and (
            df_s["annee"] == annee_lic_plus_delta
        ).any():
            l_t = float(
                df_s.loc[df_s["annee"] == annee_jo, "licences_annuelles"].iloc[0]
            )
            l_t_delta = float(
                df_s.loc[
                    df_s["annee"] == annee_lic_plus_delta, "licences_annuelles"
                ].iloc[0]
            )

            # Prévention contre une division par zéro
            if l_t > 0:
                rows.append(
                    {
                        "sport": sport,
                        "annee_licences_t": annee_jo,
                        "licences_t": l_t,
                        f"licences_t_plus_{delta}": l_t_delta,
                        "taux_croissance": 100 * (l_t_delta - l_t) / l_t,
                    }
                )

    # Construction du DataFrame final
    df_croissance = pd.DataFrame(
        rows,
        columns=[
            "sport",
            "annee_licences_t",
            "licences_t",
            f"licences_t_plus_{delta}",
            "taux_croissance",
        ],
    )

    # Classement par taux de croissance décroissant
    if not df_croissance.empty:
        df_croissance = df_croissance.sort_values(
            "taux_croissance", ascending=False
        ).reset_index(drop=True)

    return df_croissance
