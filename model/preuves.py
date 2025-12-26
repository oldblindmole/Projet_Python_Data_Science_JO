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
    df_model_full,
    train_years=(2017, 2020),
    test_years=(2022, 2024),
    medals_col="total_medailles",
    alpha=1.0,
):
    """
    Évalue des modèles Ridge en ablation (comparaison de spécifications) sur une tâche
    prédictive en log des licenciés.

    Paramètres
    ----------
    df_model_full : pandas.DataFrame
        Dataset au niveau sport-année contenant au minimum :
        - 'code_sport' : identifiant du sport (str)
        - 'annee' : année d'observation (int)
        - 'nb_licencies' : nombre de licenciés (numérique)
        - `medals_col` : variable de médailles (numérique)
    train_years : tuple[int, int], optionnel (par défaut = (2017, 2020))
        Intervalle (start, end) inclus définissant la période d'entraînement.
    test_years : tuple[int, int], optionnel (par défaut = (2022, 2024))
        Intervalle (start, end) inclus définissant la période de test.
    medals_col : str, optionnel (par défaut = "total_medailles")
        Nom de la colonne utilisée comme variable `med_last` (médailles "last JO").
    alpha : float, optionnel (par défaut = 1.0)
        Paramètre de régularisation Ridge (L2).

    Retour
    ------
    pandas.DataFrame
        Tableau récapitulatif des performances en test, avec colonnes :
        - 'model' : description de la spécification
        - 'R2_log' : R² sur la cible en log
        - 'MAE_niveau' : MAE sur la cible en niveau (après expm1)
    """
    # Tri et préparation des variables cible / features
    df = df_model_full.sort_values(["code_sport", "annee"]).copy()

    # Cible en log(1 + x)
    df["y"] = np.log1p(df["nb_licencies"])

    # Inertie : lag(1) de la cible (par sport)
    df["log_lag1"] = df.groupby("code_sport")["y"].shift(1)

    # Tendance globale
    df["trend"] = df["annee"] - df["annee"].min()

    # Médailles "last JO"
    df["med_last"] = df[medals_col]

    # Conservation des observations avec lag défini
    df = df.dropna(subset=["log_lag1"]).copy()

    # Split temporel train / test
    train_df = df[df["annee"].between(*train_years)].copy()
    test_df = df[df["annee"].between(*test_years)].copy()

    def fit_predict(feats):
        """
        Entraîne un Ridge sur un ensemble de features (avec FE sport one-hot),
        puis calcule les métriques sur le test.
        """
        # Design matrices avec effets fixes sport (one-hot)
        Xtr = pd.get_dummies( #pylint: disable=C0103
            train_df[["code_sport"] + feats], columns=["code_sport"], drop_first=True
        ).astype("float32")

        Xte = pd.get_dummies( #pylint: disable=C0103
            test_df[["code_sport"] + feats], columns=["code_sport"], drop_first=True
        ).astype("float32")

        # Alignement des colonnes entre train et test
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

    # Évaluation des spécifications (ablation)
    res = []
    res.append({"model": "M0: inertie seule (lag1)", **fit_predict(["log_lag1"])})
    res.append({"model": "M1: médailles seules", **fit_predict(["med_last"])})
    res.append(
        {"model": "M2: inertie + médailles", **fit_predict(["log_lag1", "med_last"])}
    )

    res = pd.DataFrame(res)

    return res


### a mettre dans le main###
ablation = eval_ablation_models(df_model_full)
ablation


# Preuve 2

def ridge_coefficients(
    df_model_full, train_years=(2017, 2020), medals_col="total_medailles", alpha=1.0
):
    """
    Entraîne un modèle Ridge sur la période d'entraînement et retourne
    les coefficients estimés, triés par importance absolue.

    Paramètres
    ----------
    df_model_full : pandas.DataFrame
        Dataset au niveau sport-année contenant au minimum :
        - 'code_sport' : identifiant du sport (str)
        - 'annee' : année d'observation (int)
        - 'nb_licencies' : nombre de licenciés (numérique)
        - `medals_col` : variable de médailles (numérique)
    train_years : tuple[int, int], optionnel (par défaut = (2017, 2020))
        Intervalle (start, end) inclus définissant la période utilisée
        pour l'estimation du modèle.
    medals_col : str, optionnel (par défaut = "total_medailles")
        Nom de la colonne utilisée comme variable de médailles (`med_last`).
    alpha : float, optionnel (par défaut = 1.0)
        Paramètre de régularisation Ridge (L2).

    Retour
    ------
    pandas.Series
        Série indexée par le nom des variables explicatives (features),
        contenant les coefficients du modèle Ridge, triés par valeur
        absolue décroissante.
    """
    # Tri et préparation des variables
    df = df_model_full.sort_values(["code_sport", "annee"]).copy()

    # Cible en log(1 + x)
    df["y"] = np.log1p(df["nb_licencies"])

    # Inertie : lag(1) de la cible par sport
    df["log_lag1"] = df.groupby("code_sport")["y"].shift(1)

    # Tendance linéaire globale
    df["trend"] = df["annee"] - df["annee"].min()

    # Variable de médailles associée aux JO de référence
    df["med_last"] = df[medals_col]

    # Suppression des observations sans lag
    df = df.dropna(subset=["log_lag1"]).copy()

    # Sous-échantillon d'entraînement
    train_df = df[df["annee"].between(*train_years)].copy()

    # Features numériques utilisées
    feats = ["log_lag1", "trend", "med_last"]

    # Design matrix avec effets fixes sport (one-hot)
    X = pd.get_dummies( #pylint: disable=C0103
        train_df[["code_sport"] + feats], columns=["code_sport"], drop_first=True
    ).astype("float32")

    y = train_df["y"].astype("float32").values

    # Entraînement Ridge
    m = Ridge(alpha=alpha)
    m.fit(X, y)

    # Coefficients triés par importance absolue
    coef = pd.Series(m.coef_, index=X.columns).sort_values(
        key=lambda s: s.abs(), ascending=False
    )

    return coef


### a mettre dans le main ####
coef = ridge_coefficients(df_model_full)
coef.head(15)


# Preuve 3

def test_medals_incremental(
    df_model_full, medals_col="total_medailles", start=2017, end=2024
):
    """
    Teste l'apport incrémental des médailles dans un modèle de croissance avec effets fixes.

    Paramètres
    ----------
    df_model_full : pandas.DataFrame
        Dataset au niveau sport-année contenant au minimum :
        - 'code_sport' : identifiant du sport (str)
        - 'annee' : année d'observation (int)
        - 'nb_licencies' : nombre de licenciés (numérique)
        - `medals_col` : variable de médailles (numérique)
        Optionnellement (contrôles) :
        - 'part_femmes', 'age_mean', 'nb_departements_actifs'
    medals_col : str, optionnel (par défaut = "total_medailles")
        Colonne utilisée comme variable de médailles `med_last`.
    start : int, optionnel (par défaut = 2017)
        Première année incluse dans l'estimation.
    end : int, optionnel (par défaut = 2024)
        Dernière année incluse dans l'estimation (inclusive).

    Retour
    ------
    tuple
        (m0, m1, wald) avec :
        - m0 : RegressionResultsWrapper
            Résultat statsmodels pour le modèle sans médailles.
        - m1 : RegressionResultsWrapper
            Résultat statsmodels pour le modèle avec médailles.
        - wald : ContrastResults
            Résultat du test de Wald pour H0: med_last = 0 dans m1.
    """
    # Tri et préparation des variables
    dfp = df_model_full.sort_values(["code_sport", "annee"]).copy()

    # Construction de la croissance : diff du log(1 + nb_licencies)
    dfp["log_lic"] = np.log1p(dfp["nb_licencies"])
    dfp["dlog_lic"] = dfp.groupby("code_sport")["log_lic"].diff(1)

    # Filtrage temporel + suppression des dlog manquants
    dfp = dfp[dfp["annee"].between(start, end)].dropna(subset=["dlog_lic"]).copy()

    # Variable médailles
    dfp["med_last"] = dfp[medals_col]

    # Contrôles optionnels
    controls = [
        c
        for c in ["part_femmes", "age_mean", "nb_departements_actifs"]
        if c in dfp.columns
    ]
    ctrl = (" + " + " + ".join(controls)) if controls else ""

    # Spécifications (effets fixes sport + année)
    f0 = f"dlog_lic ~ C(code_sport) + C(annee){ctrl}"
    f1 = f"dlog_lic ~ med_last + C(code_sport) + C(annee){ctrl}"

    # Estimations avec erreurs clusterisées par sport
    m0 = smf.ols(f0, data=dfp).fit(
        cov_type="cluster", cov_kwds={"groups": dfp["code_sport"]}
    )

    m1 = smf.ols(f1, data=dfp).fit(
        cov_type="cluster", cov_kwds={"groups": dfp["code_sport"]}
    )

    # Test de Wald : H0: med_last = 0
    wald = m1.wald_test("med_last = 0")
    
    return m0, m1, wald


#### a mettre dans le main####
m0, m1, wald = test_medals_incremental(df_model_full)
print(wald)
print("beta medals:", m1.params.get("med_last", np.nan))
