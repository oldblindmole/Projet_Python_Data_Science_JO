
"""
Modèle prédictif du nombre de licenciés (Ridge) et visualisation des prédictions.
Ce module entraîne un modèle Ridge sur la cible `log(1 + nb_licencies)`.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score, mean_absolute_error


def train_ridge_predictif(
    df_model_full: pd.DataFrame,
    train_years: tuple[int, int] = (2017, 2020),
    test_years: tuple[int, int] = (2022, 2024),
    medals_col: str = "total_medailles",
    alpha: float = 1.0,
    extra_controls: list[str] | None = None,
) -> dict:
    """
    Entraîne un modèle Ridge prédictif sur le log des licenciés, avec effet fixe sport
    (one-hot), une tendance par sport et un terme de dynamique via le lag.

    Modèle (log) :
        log(1+lic_t) ~ log(1+lic_{t-1}) + trend + med_last + controls + FE_sport

    Parameters
    ----------
    df_model_full : pd.DataFrame
        Dataset final contenant au minimum ['code_sport','annee','nb_licencies', medals_col].
    train_years : tuple[int, int]
        (start, end) inclus.
    test_years : tuple[int, int]
        (start, end) inclus.
    medals_col : str
        Colonne utilisée comme med_last.
    alpha : float
        Régularisation Ridge.
    extra_controls : list[str] | None
        Colonnes de contrôle optionnelles.

    Returns
    -------
    dict
        Dictionnaire contenant :
        - 'model' : Ridge
        - 'features_num' : list[str]
        - 'X_train_cols' : Index
        - 'train_df', 'test_df'
        - 'metrics' : dict (R2 log, MAE niveau)
    """
    # Tri et copie défensive
    dfs = df_model_full.sort_values(["code_sport", "annee"]).copy()

    # Cible en log(1 + x) et lag(1) de la cible (dynamique)
    dfs["log_lic"] = np.log1p(dfs["nb_licencies"])
    dfs["log_lic_lag1"] = dfs.groupby("code_sport")["log_lic"].shift(1)

    # Tendance linéaire simple propre à chaque sport : (annee - première année du sport)
    dfs["trend"] = dfs.groupby("code_sport")["annee"].transform(lambda s: s - s.min())

    # Variable médailles associée aux JO de référence
    dfs["med_last"] = dfs[medals_col]

    # On conserve uniquement les observations disposant d'un lag
    dfs = dfs.dropna(subset=["log_lic_lag1"]).copy()

    # Découpage train / test
    train_df = dfs[(dfs["annee"] >= train_years[0]) & (dfs["annee"] <= train_years[1])].copy()
    test_df = dfs[(dfs["annee"] >= test_years[0]) & (dfs["annee"] <= test_years[1])].copy()

    # Features numériques de base
    features_num = ["log_lic_lag1", "trend", "med_last"]

    # Contrôles optionnels
    if extra_controls is None:
        candidates = ["part_femmes", "age_mean", "nb_departements_actifs"]
        extra_controls = [c for c in candidates if c in dfs.columns]
    else:
        extra_controls = [c for c in extra_controls if c in dfs.columns]

    features_num += extra_controls
    features_num = [c for c in features_num if c in dfs.columns]

    # Construction des matrices X avec effets fixes sport (one-hot)
    X_train = ( #pylint: disable=C0103
        pd.get_dummies(
            train_df[["code_sport"] + features_num],
            columns=["code_sport"],
            drop_first=True,
        )
        .astype("float32")
    )

    X_test = ( #pylint: disable=C0103
        pd.get_dummies(
            test_df[["code_sport"] + features_num],
            columns=["code_sport"],
            drop_first=True,
        )
        .astype("float32")
    )

    # Alignement des colonnes train/test (sports absents du train => colonnes à 0)
    X_test = X_test.reindex(columns=X_train.columns, fill_value=0).astype("float32") #pylint: disable=C0103

    # Vecteurs cibles
    y_train = train_df["log_lic"].astype("float32").values
    y_test = test_df["log_lic"].astype("float32").values

    # Entraînement Ridge
    model = Ridge(alpha=alpha)
    model.fit(X_train, y_train)

    # Prédictions et métriques
    pred_log_test = model.predict(X_test)
    metrics = {
        "r2_log": float(r2_score(y_test, pred_log_test)),
        "mae_niveau": float(mean_absolute_error(np.expm1(y_test), np.expm1(pred_log_test))),
    }

    return {
        "model": model,
        "features_num": features_num,
        "X_train_cols": X_train.columns,
        "train_df": train_df,
        "test_df": test_df,
        "metrics": metrics,
    }


def plot_pred_sport(bundle: dict, code_sport: str):
    """
    Trace, pour un sport donné, les licenciés observés vs prédits sur la période de test.

    Paramètres
    ----------
    bundle : dict
        Sortie de train_ridge_predictif().
    code_sport : str
        Code sport (ex: "HAN").

    Retour
    ------
    pd.DataFrame | None
        DataFrame avec colonnes :
        ['code_sport','annee','nb_licencies','pred_nb_licencies']
        ou None si pas de données test.
    """
    model = bundle["model"]
    features_num = bundle["features_num"]
    X_train_cols = bundle["X_train_cols"] #pylint: disable=C0103
    test_df = bundle["test_df"]

    # Sous-échantillon : observations test du sport demandé
    d = test_df[test_df["code_sport"] == code_sport].sort_values("annee").copy()
    if d.empty:
        print(f"Aucune donnée test pour {code_sport}")
        return None

    # Construction de la matrice de features (mêmes colonnes que le train)
    Xd = ( #pylint: disable=C0103
        pd.get_dummies(
            d[["code_sport"] + features_num],
            columns=["code_sport"],
            drop_first=True,
        )
        .astype("float32")
        .reindex(columns=X_train_cols, fill_value=0)
        .astype("float32")
    )

    # Prédictions : passage log -> niveau
    d["pred_log"] = model.predict(Xd)
    d["pred_nb_licencies"] = np.expm1(d["pred_log"])

    # Affichage observé vs prédit
    plt.figure(figsize=(8, 5))
    plt.plot(d["annee"], d["nb_licencies"], marker="o", linewidth=2, label="observé")
    plt.plot(d["annee"], d["pred_nb_licencies"], marker="o", linewidth=2, label="prédit")
    plt.title(f"{code_sport} — Licenciés observé vs prédit (test)")
    plt.xlabel("Année")
    plt.ylabel("Nb licenciés")
    plt.legend()
    plt.tight_layout()
    plt.show()

    return d[["code_sport", "annee", "nb_licencies", "pred_nb_licencies"]]
