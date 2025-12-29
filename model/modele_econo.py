"""
Estimation économétrique : effet des médailles sur la croissance des licences (panel FE).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf


def fit_modele_croissance(
    df_model_full: pd.DataFrame,
    start_year: int = 2017,
    end_year: int = 2024,
    medals_col: str = "total_medailles",
):
    """
    Estime un modèle de croissance (différence du log) avec effets fixes sport et année,
    et erreurs standards clusterisées par sport.

    Spécification (panel FE) :
        dlog_lic_{s,t} = β * med_last_{s,t} + α_s + γ_t + ε_{s,t}

    où :
      - dlog_lic = log(1 + licences_annuelles_{s,t}) - log(1 + licences_annuelles_{s,t-1})
      - med_last = variable de médailles (associée au JO de référence) (colonne `medals_col`)
      - α_s : effets fixes sport (C(code_sport))
      - γ_t : effets fixes année (C(annee))
      - erreurs clusterisées par sport (corrélation intra-sport dans le temps)

    Paramètres
    ----------
    df_model_full : pd.DataFrame
        Doit contenir au minimum :
        - 'code_sport' (str)
        - 'annee' (int)
        - 'licences_annuelles' (numérique)
        - medals_col (numérique)

    start_year, end_year : int
        Fenêtre temporelle d'estimation (inclusive).

    medals_col : str
        Colonne utilisée comme variable de médailles.

    Retour
    ------
    RegressionResultsWrapper (statsmodels)
    """
    # Vérifications minimales
    required = ["code_sport", "annee", "licences_annuelles", medals_col]
    missing = [c for c in required if c not in df_model_full.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans df_model_full: {missing}")

    # Tri pour lags corrects
    dfp = df_model_full.sort_values(["code_sport", "annee"]).copy()

    # Y : croissance = diff du log(1 + licences)
    lic = pd.to_numeric(dfp["licences_annuelles"], errors="coerce")
    dfp["log_lic"] = np.log1p(lic)
    dfp["dlog_lic"] = dfp.groupby("code_sport")["log_lic"].diff(1)

    # X : médailles
    dfp["med_last"] = pd.to_numeric(dfp[medals_col], errors="coerce").fillna(0.0)

    # Fenêtre temporelle + drop 1ère année (dlog manquant)
    dfp = dfp[dfp["annee"].between(start_year, end_year)].dropna(subset=["dlog_lic"]).copy()

    # Modèle FE sport + FE année
    formula = "dlog_lic ~ med_last + C(code_sport) + C(annee)"

    # OLS + SE cluster sport
    model = smf.ols(formula, data=dfp).fit(
        cov_type="cluster",
        cov_kwds={"groups": dfp["code_sport"]},
    )

    return model
