"""
Ce module regroupe des fonctions destinées à effectuer des tests
économétriques et preuves empiriques de l'effet des médailles.
"""

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score, mean_absolute_error
import statsmodels.formula.api as smf


# Preuve 1


def eval_ablation_models(
    df_model_full: pd.DataFrame,
    train_years: tuple[int, int] = (2017, 2020),
    test_years: tuple[int, int] = (2022, 2024),
    medals_col: str = "total_medailles",
    alpha: float = 1.0,
) -> pd.DataFrame:
    """
    Évalue des modèles Ridge en ablation pour prédire le niveau de licences sportives,
    à partir d'un panel sport–année, en comparant différentes spécifications.

    Parameters
    ----------
    df_model_full : pandas.DataFrame
        Dataset panel au niveau (code_sport, annee) contenant au minimum :
        - 'code_sport' : identifiant du sport (str)
        - 'annee' : année d'observation (int)
        - 'licences_annuelles' : nombre de licences agrégées (numérique)
        - `medals_col` : variable de médailles associée aux JO de référence

    train_years : tuple[int, int], optional (default=(2017, 2020))
        Intervalle (start, end) inclus définissant la période d'entraînement.

    test_years : tuple[int, int], optional (default=(2022, 2024))
        Intervalle (start, end) inclus définissant la période de test.

    medals_col : str, optional (default="total_medailles")
        Nom de la colonne utilisée comme variable explicative principale
        (médailles obtenues lors des JO de référence).

    alpha : float, optional (default=1.0)
        Paramètre de régularisation L2 du modèle Ridge.

    Returns
    -------
    pandas.DataFrame
        Tableau récapitulatif des performances en test, avec les colonnes :
        - 'model' : description de la spécification estimée
        - 'R2_log' : coefficient de détermination R² sur la cible en log
        - 'MAE_niveau' : erreur absolue moyenne sur la cible en niveau
          (après transformation inverse expm1)
    """
    df = df_model_full.sort_values(["code_sport", "annee"]).copy()

    # Cible en log(1 + x)
    df["y"] = np.log1p(df["licences_annuelles"])

    # Inertie : lag(1) de la cible par sport
    df["log_lag1"] = df.groupby("code_sport")["y"].shift(1)

    # Médailles "last JO"
    df["med_last"] = df[medals_col]

    # Conservation des observations avec lag défini
    df = df.dropna(subset=["log_lag1"]).copy()

    # Split temporel
    train_df = df[df["annee"].between(*train_years)].copy()
    test_df = df[df["annee"].between(*test_years)].copy()

    def fit_predict(feats: list[str]) -> dict[str, float]:
        Xtr = pd.get_dummies( #pylint: disable=C0103
            train_df[["code_sport"] + feats],
            columns=["code_sport"],
            drop_first=True,
        ).astype("float32")

        Xte = pd.get_dummies( #pylint: disable=C0103
            test_df[["code_sport"] + feats],
            columns=["code_sport"],
            drop_first=True,
        ).astype("float32")

        Xte = Xte.reindex(columns=Xtr.columns, fill_value=0).astype("float32") #pylint: disable=C0103

        ytr = train_df["y"].astype("float32").values
        yte = test_df["y"].astype("float32").values

        m = Ridge(alpha=alpha)
        m.fit(Xtr, ytr)
        pred = m.predict(Xte)

        return {
            "R2_log": float(r2_score(yte, pred)),
            "MAE_niveau": float(mean_absolute_error(np.expm1(yte), np.expm1(pred))),
        }

    res = []
    res.append({"model": "M0: inertie seule (lag1)", **fit_predict(["log_lag1"])})
    res.append({"model": "M1: médailles seules", **fit_predict(["med_last"])})
    res.append(
        {"model": "M2: inertie + médailles", **fit_predict(["log_lag1", "med_last"])}
    )

    return pd.DataFrame(res)


# Preuve 2


def ridge_coefficients(
    df_model_full: pd.DataFrame,
    train_years: tuple[int, int] = (2017, 2020),
    medals_col: str = "total_medailles",
    alpha: float = 1.0,
) -> pd.Series:
    """
    Entraîne un modèle Ridge (régression L2) sur une période d'entraînement et
    retourne les coefficients estimés, triés par importance absolue.

    Paramètres
    ----------
    df_model_full : pandas.DataFrame
        Dataset au niveau sport-année contenant au minimum :
        - 'code_sport' : identifiant du sport (str)
        - 'annee' : année d'observation (int)
        - 'licences_annuelles' : nombre de licences / licenciés (numérique)
        - `medals_col` : variable de médailles (numérique)
    train_years : tuple[int, int], optionnel (par défaut = (2017, 2020))
        Intervalle (start, end) inclus définissant la période utilisée
        pour l'estimation du modèle.
    medals_col : str, optionnel (par défaut = "total_medailles")
        Nom de la colonne utilisée comme variable de médailles (`med_last`).
    alpha : float, optionnel (par défaut = 1.0)
        Paramètre de régularisation Ridge (L2). Plus alpha est grand,
        plus les coefficients sont pénalisés et "rétrécis".

    Retour
    ------
    pandas.Series
        Série indexée par le nom des variables explicatives (features),
        contenant les coefficients du modèle Ridge, triés par valeur
        absolue décroissante.
    """
    # Tri et copie défensive
    df = df_model_full.sort_values(["code_sport", "annee"]).copy()

    # Vérification minimale des colonnes requises
    required = ["code_sport", "annee", "licences_annuelles", medals_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans df_model_full: {missing}")

    # Cible en log(1 + licences_annuelles)
    df["y"] = np.log1p(pd.to_numeric(df["licences_annuelles"], errors="coerce").fillna(0.0))

    # Inertie : lag(1) de la cible par sport
    df["log_lag1"] = df.groupby("code_sport")["y"].shift(1)

    # Tendance linéaire globale (0 au min)
    df["trend"] = df["annee"].astype(int) - int(df["annee"].min())

    # Médailles "last JO" (déjà alignées dans ton dataset via jo_ref)
    df["med_last"] = pd.to_numeric(df[medals_col], errors="coerce").fillna(0.0)

    # Suppression des observations sans lag (première année par sport)
    df = df.dropna(subset=["log_lag1"]).copy()

    # Sous-échantillon d'entraînement
    train_df = df[df["annee"].between(*train_years)].copy()

    # Features numériques utilisées
    feats = ["log_lag1", "trend", "med_last"]

    # Design matrix avec effets fixes sport (one-hot)
    X = pd.get_dummies( #pylint: disable=C0103
        train_df[["code_sport"] + feats],
        columns=["code_sport"],
        drop_first=True,
    ).astype("float32")

    y = train_df["y"].astype("float32").values

    # Entraînement Ridge
    model = Ridge(alpha=alpha)
    model.fit(X, y)

    # Coefficients triés par importance absolue
    coef = pd.Series(model.coef_, index=X.columns).sort_values(
        key=lambda s: s.abs(), ascending=False
    )

    return coef


# Preuve 3


def test_medals_incremental(
    df_model_full: pd.DataFrame,
    medals_col: str = "total_medailles",
    start: int = 2017,
    end: int = 2024,
):
    """
    Teste l'apport incrémental des médailles olympiques dans un modèle
    de croissance des licences, avec effets fixes sport et année.

    Paramètres
    ----------
    df_model_full : pandas.DataFrame
        Dataset au niveau sport–année contenant au minimum :
        - 'code_sport' : identifiant du sport (str)
        - 'annee' : année d'observation (int)
        - 'licences_annuelles' : nombre de licences (numérique)
        - `medals_col` : variable de médailles (numérique)

        Optionnellement, les contrôles suivants sont utilisés s'ils sont présents :
        - 'part_femmes'
        - 'age_mean'
        - 'nb_departements_actifs'

    medals_col : str, optionnel (par défaut = "total_medailles")
        Colonne utilisée comme variable explicative principale des médailles
        (médailles associées aux JO de référence).

    start : int, optionnel (par défaut = 2017)
        Première année incluse dans l'estimation.

    end : int, optionnel (par défaut = 2024)
        Dernière année incluse dans l'estimation (inclusive).

    Retour
    ------
    tuple
        (m0, m1, wald) avec :
        - m0 : RegressionResultsWrapper
            Résultat du modèle sans médailles.
        - m1 : RegressionResultsWrapper
            Résultat du modèle avec médailles.
        - wald : ContrastResults
            Résultat du test de Wald pour l'hypothèse nulle H0 : med_last = 0.
    """
    # Tri pour garantir des lags corrects par sport
    dfp = df_model_full.sort_values(["code_sport", "annee"]).copy()

    # Vérification minimale des colonnes requises
    required = ["code_sport", "annee", "licences_annuelles", medals_col]
    missing = [c for c in required if c not in dfp.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans df_model_full: {missing}")

    # Construction de la croissance : diff du log(1 + licences_annuelles)
    dfp["log_lic"] = np.log1p(
        pd.to_numeric(dfp["licences_annuelles"], errors="coerce").fillna(0.0)
    )
    dfp["dlog_lic"] = dfp.groupby("code_sport")["log_lic"].diff(1)

    # Filtrage temporel et suppression de la première année par sport
    dfp = (
        dfp[dfp["annee"].between(start, end)]
        .dropna(subset=["dlog_lic"])
        .copy()
    )

    # Variable médailles
    dfp["med_last"] = pd.to_numeric(dfp[medals_col], errors="coerce").fillna(0.0)

    # Contrôles optionnels
    controls = [
        c
        for c in ["part_femmes", "age_mean", "nb_departements_actifs"]
        if c in dfp.columns
    ]
    ctrl = (" + " + " + ".join(controls)) if controls else ""

    # Spécifications avec effets fixes sport et année
    f0 = f"dlog_lic ~ C(code_sport) + C(annee){ctrl}"
    f1 = f"dlog_lic ~ med_last + C(code_sport) + C(annee){ctrl}"


    # Estimation avec erreurs standards clusterisées par sport

    #m0 = regression de référence (= m1 quand med_last = 0)
    # m0 = smf.ols(f0, data=dfp).fit(
    #     cov_type="cluster",
    #     cov_kwds={"groups": dfp["code_sport"]},
    # )

    #m1 = la régression avec médailles
    m1 = smf.ols(f1, data=dfp).fit(
        cov_type="cluster",
        cov_kwds={"groups": dfp["code_sport"]},
    )

    # Test de Wald : H0 : coefficient des médailles = 0
    wald = m1.wald_test("med_last = 0")

    return  m1, wald
