"""
Construction et enrichissement du dataset de modélisation des licenciés sportifs.

Ce module implémente l'ensemble des étapes de feature engineering nécessaires
à la construction d'un panel sport–année exploitable pour l'analyse
économétrique et la modélisation prédictive du nombre de licenciés.
"""

import numpy as np
import pandas as pd


# Licences


def agreg_licencies_par_sport_annee(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège le nombre de licences par sport et par année.

    Paramètre
    ----------
    df : pandas.DataFrame
        DataFrame contenant au minimum :
        ['code_sport', 'annee', 'licences_annuelles'].

    Retours
    -------
    pandas.DataFrame
        DataFrame agrégé avec :
        ['code_sport', 'annee', 'licences_annuelles'].
    """
    return df.groupby(["code_sport", "annee"], as_index=False).agg(
        licences_annuelles=("licences_annuelles", "sum")
    )


def safe_div(a, b):
    """
    Division élément par élément en évitant les divisions par zéro.
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    return np.where(b == 0, 0.0, a / b)


def build_lic_features(df_lic: pd.DataFrame) -> pd.DataFrame:
    """
    Construit des features liées aux licences et ajoute des lags /
    croissances au niveau (code_sport, annee).
    """
    df = df_lic.copy()

    # Vérification des colonnes minimales
    required = ["code_sport", "annee", "licences_annuelles"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans df: {missing}")

    # Normalisation
    df["annee"] = df["annee"].astype(int)
    df["licences_annuelles"] = pd.to_numeric(
        df["licences_annuelles"], errors="coerce"
    ).fillna(0.0)

    # Base agrégée sport-année
    out = (
        df.groupby(["code_sport", "annee"], as_index=False)
        .agg(licences_annuelles=("licences_annuelles", "sum"))
        .sort_values(["code_sport", "annee"])
    )

    # Sexe
          
    # Sexe (colonne connue : "sexe", modalités : "F" / "H")
    s = df["sexe"].astype(str).str.strip().str.lower()

    df["_is_femme"] = (s == "f").astype(int)
    df["_is_homme"] = (s == "h").astype(int)

    sex = (
        df.groupby(["code_sport", "annee"], as_index=False)
        .apply(
            lambda x: pd.Series(
            {
                "nb_femmes": (x["licences_annuelles"] * x["_is_femme"]).sum(),
                "nb_hommes": (x["licences_annuelles"] * x["_is_homme"]).sum(),
            }
            )
        )
    )

    out = out.merge(sex, on=["code_sport", "annee"], how="left").fillna(0.0)

    out["part_femmes"] = safe_div(out["nb_femmes"], out["nb_femmes"] + out["nb_hommes"])

    # Nettoyage des colonnes temporaires dans df (pas dans out)
    df.drop(columns=["_is_femme", "_is_homme"], inplace=True, errors="ignore")
    


    # Âge

    age_col = next((c for c in ["age", "âge", "Age", "AGE"] if c in df.columns), None)

    if age_col is not None:
        df["_age"] = pd.to_numeric(df[age_col], errors="coerce")

        df["is_jeune_lt14"] = (df["_age"] < 14).astype(int)
        df["is_jeune_14_17"] = ((df["_age"] >= 14) & (df["_age"] <= 17)).astype(int)
        df["is_adulte_18_34"] = ((df["_age"] >= 18) & (df["_age"] <= 34)).astype(int)
        df["is_adulte_35_49"] = ((df["_age"] >= 35) & (df["_age"] <= 49)).astype(int)
        df["is_senior_50p"] = (df["_age"] >= 50).astype(int)

        def _age_block(x: pd.DataFrame) -> pd.Series:
            w = x["licences_annuelles"].to_numpy(dtype=float)
            a = x["_age"].to_numpy(dtype=float)

            ok = np.isfinite(a)
            w2, a2 = w[ok], a[ok]

            if w2.sum() == 0:
                age_mean, age_std = np.nan, np.nan
            else:
                age_mean = (a2 * w2).sum() / w2.sum()
                age2_mean = ((a2**2) * w2).sum() / w2.sum()
                age_std = np.sqrt(max(0.0, age2_mean - age_mean**2))

            return pd.Series(
                {
                    "age_mean": age_mean,
                    "age_std": age_std,
                    "nb_lt14": x.loc[
                        x["is_jeune_lt14"] == 1, "licences_annuelles"
                    ].sum(),
                    "nb_14_17": x.loc[
                        x["is_jeune_14_17"] == 1, "licences_annuelles"
                    ].sum(),
                    "nb_18_34": x.loc[
                        x["is_adulte_18_34"] == 1, "licences_annuelles"
                    ].sum(),
                    "nb_35_49": x.loc[
                        x["is_adulte_35_49"] == 1, "licences_annuelles"
                    ].sum(),
                    "nb_50p": x.loc[
                        x["is_senior_50p"] == 1, "licences_annuelles"
                    ].sum(),
                }
            )

        age_feat = (
            df.groupby(["code_sport", "annee"])
            .apply(_age_block)
            .reset_index()
        )
        out = out.merge(age_feat, on=["code_sport", "annee"], how="left")

        tranche_cols = ["nb_lt14", "nb_14_17", "nb_18_34", "nb_35_49", "nb_50p"]
        for c in tranche_cols:
            out[f"part_{c.replace('nb_', '')}"] = safe_div(
                out[c], out["licences_annuelles"]
            )

        df.drop(
            columns=[
                "_age",
                "is_jeune_lt14",
                "is_jeune_14_17",
                "is_adulte_18_34",
                "is_adulte_35_49",
                "is_senior_50p",
            ],
            inplace=True,
            errors="ignore",
        )

    # Lags / croissances / moyenne glissante

    out["licences_annuelles_lag1"] = out.groupby("code_sport")[
        "licences_annuelles"
    ].shift(1)
    out["licences_annuelles_lag2"] = out.groupby("code_sport")[
        "licences_annuelles"
    ].shift(2)

    out["croissance_lag1"] = safe_div(
        out["licences_annuelles"] - out["licences_annuelles_lag1"],
        out["licences_annuelles_lag1"],
    )
    out["croissance_lag2"] = safe_div(
        out["licences_annuelles_lag1"] - out["licences_annuelles_lag2"],
        out["licences_annuelles_lag2"],
    )

    out["licences_annuelles_roll2"] = (
        out.groupby("code_sport")["licences_annuelles"]
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
    à une année d'observation.

    Paramètres
    ----------
    annee : int
        Année d'observation.

    Retour
    ------
    int
        Année des Jeux Olympiques de référence (2016, 2020 ou 2024).
    """
    if annee <= 2020:
        return 2016
    elif annee <= 2024:
        return 2020
    # else:
    #     return 2024


def ajouter_jo_ref(df: pd.DataFrame, annee_col: str = "annee") -> pd.DataFrame:
    """
    Ajoute une colonne indiquant l'année des Jeux Olympiques de référence
    associée à chaque observation.

    Paramètres
    ----------
    df : pandas.DataFrame
        DataFrame contenant une colonne d'année.
    annee_col : str, optionnel (par défaut = "annee")
        Nom de la colonne contenant l'année d'observation.

    Retour
    ------
    pandas.DataFrame
        Copie du DataFrame d'entrée enrichie d'une colonne :
        - 'jo_ref' : année des Jeux Olympiques de référence (2016, 2020 ou 2024).
    """
    out = df.copy()

    # Application de la règle de rattachement aux JO
    out["jo_ref"] = out[annee_col].apply(jo_reference)

    return out


# Médailles


def build_df_sport(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construit un DataFrame au niveau du sport, avec une ligne par `code_sport`
    et les colonnes de médailles par olympiade.

    Paramètres
    ----------
    df : pandas.DataFrame
        DataFrame source contenant au minimum :
        - 'code_sport' : code du sport
        - 'sport' : libellé du sport
        - les colonnes de médailles :
          * '2016_or', '2016_argent', '2016_bronze', 'total_medailles_2016'
          * '2020_or', '2020_argent', '2020_bronze', 'total_medailles_2020'
          * '2024_or', '2024_argent', '2024_bronze', 'total_medailles_2024'

    Retour
    ------
    pandas.DataFrame
        DataFrame avec une ligne par `code_sport`, contenant :
        - 'code_sport', 'sport'
        - les colonnes de médailles par olympiade.
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

    # Déduplication : une ligne par sport
    return df.drop_duplicates(subset=["code_sport"])[cols].copy()


def build_df_med_long(df_sport: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme les colonnes de médailles au format wide (par olympiade)
    en un format long au niveau (code_sport, jo_ref).

    Paramètres
    ----------
    df_sport : pandas.DataFrame
        DataFrame au niveau du sport, typiquement issu de `build_df_sport`,
        contenant au minimum :
        - 'code_sport', 'sport'
        - '{annee}_or', '{annee}_argent', '{annee}_bronze'
        - 'total_medailles_{annee}'
        pour annee ∈ {2016, 2020, 2024}.

    Retour
    ------
    pandas.DataFrame
        DataFrame long contenant les colonnes :
        - 'code_sport'
        - 'sport'
        - 'jo_ref' : année des JO (2016, 2020, 2024)
        - 'or', 'argent', 'bronze'
        - 'total_medailles'
    """
    def _block(year: int) -> pd.DataFrame:
        """
        Extrait et renomme les colonnes de médailles pour une édition donnée
        des Jeux Olympiques.
        """
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

    # Empilement des blocs correspondant aux différentes olympiades
    return pd.concat([_block(2016), _block(2020), _block(2024)], ignore_index=True)


def merge_medals(df_agg: pd.DataFrame, df_med: pd.DataFrame) -> pd.DataFrame:
    """
    Fusionne les informations de médailles dans un panel sport-année.

    Paramètres
    ----------
    df_agg : pandas.DataFrame
        Panel au niveau sport-année contenant au minimum :
        - 'code_sport'
        - 'annee'
        - 'jo_ref'
    df_med : pandas.DataFrame
        Table de médailles au niveau (code_sport, jo_ref) contenant :
        - 'code_sport'
        - 'jo_ref'
        - 'or', 'argent', 'bronze'
        - 'total_medailles'

    Retour
    ------
    pandas.DataFrame
        DataFrame `df_agg` enrichi des colonnes de médailles :
        - 'or', 'argent', 'bronze', 'total_medailles'
        Les valeurs manquantes sont remplacées par 0.
    """

    # Fusion sur la clé (code_sport, jo_ref)
    out = df_agg.merge(
        df_med[["code_sport", "jo_ref", "or", "argent", "bronze", "total_medailles"]],
        on=["code_sport", "jo_ref"],
        how="left",
    )

    # Imputation : absence de médailles => 0
    for c in ["or", "argent", "bronze", "total_medailles"]:
        out[c] = out[c].fillna(0)

    return out


# Dataset final


def build_model_dataset(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Construit le dataset final de modélisation au niveau (code_sport, annee)
    à partir d'un DataFrame brut.

    Paramètres
    ----------
    df_raw : pandas.DataFrame
        DataFrame brut, typiquement chargé depuis un fichier parquet.
        Il doit contenir au minimum :
        - 'code_sport', 'annee', 'licences_annuelles'
        Et, pour les médailles (si utilisées) :
        - 'sport' + colonnes de médailles par olympiade
          (ex : '2016_or', '2020_bronze', 'total_medailles_2024', ...)
        Et, optionnellement pour enrichir les features :
        - une colonne de sexe parmi {'sexe', 'genre', 'sex', 'gender'}
        - une colonne d'âge parmi {'age', 'âge', 'Age', 'AGE'}

    Retour
    ------
    pandas.DataFrame
        Dataset final au niveau (code_sport, annee), contenant :
        - 'code_sport', 'annee'
        - 'nb_licencies'
        - 'jo_ref'
        - médailles associées à 'jo_ref' : 'or', 'argent', 'bronze', 'total_medailles'
        - features issues de `build_lic_features` (âge, sexe, lags, croissances, etc.)
          lorsque ces informations sont disponibles dans `df_raw`.
    """

    # Agrégation des licenciés au niveau (code_sport, annee)
    df_agg = agreg_licencies_par_sport_annee(df_raw)

    # Ajout de l'année JO de référence pour chaque année d'observation
    df_agg = ajouter_jo_ref(df_agg, annee_col="annee")

    # Construction et fusion des médailles au niveau (code_sport, jo_ref)
    df_sport = build_df_sport(df_raw)
    df_med = build_df_med_long(df_sport)
    df_model = merge_medals(df_agg, df_med)

    # Features additionnelles issues du brut (sexe, âge, lags, croissances…)
    lic_feat = build_lic_features(df_raw)

    # Fusion finale
    df_final = df_model.merge(
        lic_feat.drop(columns=["licences_annuelles"], errors="ignore"),
        on=["code_sport", "annee"],
        how="left",
    )
    df_final.drop(columns=["nb_femmes", "nb_hommes"], inplace=True, errors="ignore")
    return df_final
