"""
Feature engineering et construction du dataset de modélisation.
Ce module construit un panel au niveau (code_sport, annee) à partir d'un DataFrame brut
de licences
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# Licences

def agreg_licencies_par_sport_annee(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège le nombre de licenciés par sport et par année.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame contenant au minimum les colonnes
        ['code_sport', 'annee', 'licences_annuelles'].

    Returns
    -------
    pandas.DataFrame
        DataFrame agrégé avec les colonnes
        ['code_sport', 'annee', 'licences_annuelles'].
    """
    df_agg = df.copy()

    # Renommage explicite de la variable à agréger
    #df_agg = df_agg.rename(columns={"licences_annuelles": "nb_licencies"})

    # Agrégation par sport et par année
    df_agg = df_agg.groupby(["code_sport", "annee"], as_index=False).agg(
        {"licences_annuelles": "sum"}
    )

    return df_agg


def safe_div(a, b):
    """
     Effectue une division élément par élément en évitant les divisions par zéro.

    Paramètres
    ----------
    a : array-like
    b : array-like

    Retour
    ------
    array-like
    Retourne a/b, mais met 0 si b == 0.
    """
    # Conversion en tableaux NumPy
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)

    return np.where(b == 0, 0.0, a / b)


def build_lic_features(df_lic: pd.DataFrame) -> pd.DataFrame:
    """
    Construit des variables exlicatives (features) additionnelles liées
    aux licenciés (âge, sexe, tranches d’âge, etc.) et ajoute des lags / croissances
    au niveau (code_sport, annee).

    Parameters
    ----------
    df_lic : pd.DataFrame
        DataFrame contenant au minimum :
        ['code_sport', 'annee', 'licences_annuelles'].
        Si disponibles, la fonction exploite aussi des colonnes comme :
        - sexe / genre (ex: 'sexe', 'genre', etc.)
        - âge (ex: 'age')
        - département (ex: 'departement')

    Returns
    -------
    pd.DataFrame
        DataFrame au niveau (code_sport, annee) contenant :
        - 'nb_licencies'
        - features (ex: part_femmes, age_mean, age_std, tranches d’âge…)
        - lags (nb_licencies_lag1, nb_licencies_lag2)
        - croissances (croissance_lag1, croissance_lag2)
        - moyenne glissante (nb_licencies_roll2)
    """
    df = df_lic.copy()

    # Vérification des colonnes minimales requises
    required = ["code_sport", "annee", "licences_annuelles"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans df: {missing}")

    # Normalisation des types des variables de base
    df["annee"] = df["annee"].astype(int)
    df["licences_annuelles"] = pd.to_numeric(
        df["licences_annuelles"], errors="coerce"
    ).fillna(0.0)

    # Base agrégée (niveau sport-année)
    base = df.groupby(["code_sport", "annee"], as_index=False).agg(
        nb_licencies=("licences_annuelles", "sum")
    )

    out = base.copy()

    # Feature de sexe
    # On essaie de détecter une colonne de sexe courante
    sex_col = None
    for c in ["sexe", "genre", "sex", "gender"]:
        if c in df.columns:
            sex_col = c
            break

    if sex_col is not None:
        # Normalisation en minuscules pour faciliter la reconnaissance
        s = df[sex_col].astype(str).str.lower()

        # Indicateurs "femme" / "homme"
        df["_is_femme"] = s.isin(["f", "femme", "female", "women", "woman"]).astype(int)
        df["_is_homme"] = s.isin(["m", "homme", "male", "men", "man"]).astype(int)

        # Agrégation pondérée par le volume de licences
        sex = (
            df.groupby(["code_sport", "annee"])
            .apply(
                lambda x: pd.Series(
                    {
                        "nb_femmes": float(
                            (x["licences_annuelles"] * x["_is_femme"]).sum()
                        ),
                        "nb_hommes": float(
                            (x["licences_annuelles"] * x["_is_homme"]).sum()
                        ),
                    }
                )
            )
            .reset_index()
        )

        out = out.merge(sex, on=["code_sport", "annee"], how="left")
        out[["nb_femmes", "nb_hommes"]] = out[["nb_femmes", "nb_hommes"]].fillna(0.0)

        # Part de femmes parmi les individus reconnus
        out["part_femmes"] = safe_div(
            out["nb_femmes"], (out["nb_femmes"] + out["nb_hommes"])
        )

        # Nettoyage des colonnes temporaires
        df = df.drop(columns=["_is_femme", "_is_homme"], errors="ignore")

    # Features d'âge
    age_col = None
    for c in ["age", "âge", "Age", "AGE"]:
        if c in df.columns:
            age_col = c
            break

    if age_col is not None:
        df["_age"] = pd.to_numeric(df[age_col], errors="coerce")

        # Définition de tranches d'âge
        df["is_jeune_lt14"] = (df["_age"] < 14).astype(int)
        df["is_jeune_14_17"] = ((df["_age"] >= 14) & (df["_age"] <= 17)).astype(int)
        df["is_adulte_18_34"] = ((df["_age"] >= 18) & (df["_age"] <= 34)).astype(int)
        df["is_adulte_35_49"] = ((df["_age"] >= 35) & (df["_age"] <= 49)).astype(int)
        df["is_senior_50p"] = (df["_age"] >= 50).astype(int)

        def _age_block(x: pd.DataFrame) -> pd.Series:
            """
            Calcule des statistiques d'âge (moyenne/écart-type) et des effectifs
            par tranches, en pondérant par 'licences_annuelles'.
            """
            w = x["licences_annuelles"].to_numpy(dtype=float)
            a = x["_age"].to_numpy(dtype=float)

            # On ne conserve que les âges valides pour le calcul des stats
            ok = np.isfinite(a)
            w2 = w[ok]
            a2 = a[ok]

            # Statistiques d'âge pondérées
            if w2.sum() == 0:
                age_mean = np.nan
                age_std = np.nan
            else:
                age_mean = float((a2 * w2).sum() / w2.sum())
                age2_mean = float(((a2**2) * w2).sum() / w2.sum())
                var = max(0.0, age2_mean - age_mean**2)
                age_std = float(np.sqrt(var))

            # Effectifs (pondérés) par tranche
            nb_lt14 = float(x.loc[x["is_jeune_lt14"] == 1, "licences_annuelles"].sum())
            nb_14_17 = float(
                x.loc[x["is_jeune_14_17"] == 1, "licences_annuelles"].sum()
            )
            nb_18_34 = float(
                x.loc[x["is_adulte_18_34"] == 1, "licences_annuelles"].sum()
            )
            nb_35_49 = float(
                x.loc[x["is_adulte_35_49"] == 1, "licences_annuelles"].sum()
            )
            nb_50p = float(x.loc[x["is_senior_50p"] == 1, "licences_annuelles"].sum())

            return pd.Series(
                {
                    "age_mean": age_mean,
                    "age_std": age_std,
                    "nb_lt14": nb_lt14,
                    "nb_14_17": nb_14_17,
                    "nb_18_34": nb_18_34,
                    "nb_35_49": nb_35_49,
                    "nb_50p": nb_50p,
                }
            )

        age_feat = df.groupby(["code_sport", "annee"]).apply(_age_block).reset_index()
        out = out.merge(age_feat, on=["code_sport", "annee"], how="left")

        # # Parts par tranche (si nb_licencies == 0 => 0 via safe_div)
        tranche_cols = ["nb_lt14", "nb_14_17", "nb_18_34", "nb_35_49", "nb_50p"]
        for c in tranche_cols:
            if c in out.columns:
                out[f"part_{c.replace('nb_', '')}"] = safe_div(
                    out[c], out["nb_licencies"]
                )

        # Nettoyage des colonnes temporaires
        df = df.drop(
            columns=[
                "_age",
                "is_jeune_lt14",
                "is_jeune_14_17",
                "is_adulte_18_34",
                "is_adulte_35_49",
                "is_senior_50p",
            ],
            errors="ignore",
        )

    # Features temporelles : lags / croissances / moyenne glissante
    out = out.sort_values(["code_sport", "annee"]).copy()

    # Lags (par sport)
    out["nb_licencies_lag1"] = out.groupby("code_sport")["nb_licencies"].shift(1)
    out["nb_licencies_lag2"] = out.groupby("code_sport")["nb_licencies"].shift(2)

    # Croissances (taux)
    out["croissance_lag1"] = safe_div(
        out["nb_licencies"] - out["nb_licencies_lag1"], out["nb_licencies_lag1"]
    )
    out["croissance_lag2"] = safe_div(
        out["nb_licencies_lag1"] - out["nb_licencies_lag2"], out["nb_licencies_lag2"]
    )

    # Moyenne glissante 2 ans
    out["nb_licencies_roll2"] = (
        out.groupby("code_sport")["nb_licencies"]
        .shift(1)
        .rolling(2)
        .mean()
        .reset_index(level=0, drop=True)
    )

    return out


# JO

def jo_reference(annee: int) -> int:
    """
    Renvoie l'année des Jeux Olympiques de référence associée
    à une année d'observation donnée.

    Paramètres
    ----------
    annee : int

    Retour
    ------
    int
    2016 si annee <= 2020, 2020 si 2021-2023, sinon 2024.
    """
    if annee <= 2020:
        return 2016
    elif annee < 2024:
        return 2020
    else:
        return 2024


def ajouter_jo_ref(df: pd.DataFrame, annee_col: str = "annee") -> pd.DataFrame:
    """
    Ajoute une colonne 'jo_ref' dans un DataFrame à partir d'une colonne année.

    Parameters
    ----------
    df : pd.DataFrame
    annee_col : str

    Returns
    -------
    pd.DataFrame
        Copie de df avec une colonne 'jo_ref'.
    """
    out = df.copy()

    # Application de la règle de rattachement aux JO
    out["jo_ref"] = out[annee_col].apply(jo_reference)

    return out


# Médailles

def build_df_sport(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construit un DataFrame unique par sport avec les colonnes de médailles par olympiade.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame source contenant au minimum 'code_sport', 'sport'
        et les colonnes de médailles 2016/2020/2024.

    Returns
    -------
    pd.DataFrame
        Une ligne par 'code_sport' avec les colonnes médailles.
    """
    cols = [
        "code_sport",
        "sport",
        "2016_or",
        "2016_argent",
        "2016_bronze",
        "total_medailles_2016",
        "2020_or",
        "2020_argent",
        "2020_bronze",
        "total_medailles_2020",
        "2024_or",
        "2024_argent",
        "2024_bronze",
        "total_medailles_2024",
    ]

    # Déduplication : une ligne par code_sport
    return df.drop_duplicates(subset=["code_sport"])[cols].copy()


def build_df_med_long(df_sport: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit les colonnes de médailles au format wide en un format long.

    Parameters
    ----------
    df_sport : pd.DataFrame
        DataFrame unique par sport, typiquement sortie de build_df_sport().

    Returns
    -------
    pd.DataFrame
        DataFrame long avec colonnes :
        ['code_sport', 'sport', 'jo_ref', 'or', 'argent', 'bronze', 'total_medailles'].
    """
    # Construction d'un bloc (une édition des JO) et renommage vers un schéma commun
    def _block(year: int) -> pd.DataFrame:
        return (
            df_sport[
                [
                    "code_sport",
                    "sport",
                    f"{year}_or",
                    f"{year}_argent",
                    f"{year}_bronze",
                    f"total_medailles_{year}",
                ]
            ]
            .rename(
                columns={
                    f"{year}_or": "or",
                    f"{year}_argent": "argent",
                    f"{year}_bronze": "bronze",
                    f"total_medailles_{year}": "total_medailles",
                }
            )
            .assign(jo_ref=year)
        )

    # Empilement des blocs 2016 / 2020 / 2024
    df_med = pd.concat([_block(2016), _block(2020), _block(2024)], ignore_index=True)

    return df_med


def merge_medals(df_agg: pd.DataFrame, df_med: pd.DataFrame) -> pd.DataFrame:
    """
    Fusionne les informations de médailles dans un panel sport-année.
    La fusion est effectuée sur la clé (`code_sport`, `jo_ref`).

    Parameters
    ----------
    df_agg : pd.DataFrame
        Doit contenir ['code_sport', 'annee', 'jo_ref'].
    df_med : pd.DataFrame
        Doit contenir ['code_sport', 'jo_ref', 'or', 'argent', 'bronze', 'total_medailles'].

    Returns
    -------
    pd.DataFrame
        Panel enrichi, avec médailles à 0 si manquantes.
    """
    # Merge sur la clé (code_sport, jo_ref)
    out = df_agg.merge(
        df_med[["code_sport", "jo_ref", "or", "argent", "bronze", "total_medailles"]],
        on=["code_sport", "jo_ref"],
        how="left",
    )

    # Imputation : si médailles manquantes, on considère 0
    for c in ["or", "argent", "bronze", "total_medailles"]:
        out[c] = out[c].fillna(0)

    return out


# Dataset final

def build_model_dataset(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Construit le dataset final de modélisation au niveau (code_sport, annee)
    à partir d'un DataFrame brut.

    Parameters
    ----------
    df_raw : pd.DataFrame
        DataFrame brut (celui chargé depuis parquet dans ton notebook).

    Returns
    -------
    pd.DataFrame
        Dataset final au niveau (code_sport, annee), avec :
        - nb_licencies
        - jo_ref
        - médailles de jo_ref
        - features licences (âge, sexe, lags, croissances…) si possibles
    """
    # Panel licenciés au niveau (code_sport, annee)
    df_agg = agreg_licencies_par_sport_annee(df_raw)

    # Ajout de l'année JO de référence pour chaque année d'observation
    df_agg = ajouter_jo_ref(df_agg, annee_col="annee")

    # Construction et fusion des médailles au niveau (code_sport, jo_ref)
    df_sport = build_df_sport(df_raw)
    df_med = build_df_med_long(df_sport)
    df_model = merge_medals(df_agg, df_med)

    # Features additionnelles issues du brut (sexe, âge, lags, croissances…)
    lic_feat = build_lic_features(df_raw)

    # Fusion finale (on évite le doublon sur nb_licencies)
    df_final = df_model.merge(
        lic_feat.drop(columns=["nb_licencies"], errors="ignore"),
        on=["code_sport", "annee"],
        how="left",
    )

    return df_final
