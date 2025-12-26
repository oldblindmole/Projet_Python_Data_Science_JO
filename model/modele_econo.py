"""
Fonction qui créer le modèle écononmétrique pour déterminer l'effet des médailles sur les prédictions
du nombre de licencié
"""


from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf


def fit_modele_croissance(
    df_model_full: pd.DataFrame,
    start_year: int = 2017,
    end_year: int = 2024,
    controls: list[str] | None = None,
    medals_col: str = "total_medailles",
) -> object:
    """
    Estime un modèle “écono” en croissance (diff log) avec effets fixes sport + année
    et erreurs clusterisées par sport.

    Modèle :
        dlog_lic ~ med_last + controls + FE_sport + FE_annee

    Parameters
    ----------
    df_model_full : pd.DataFrame
        Dataset final contenant au moins :
        ['code_sport', 'annee', 'nb_licencies', medals_col]
    start_year : int
    end_year : int
    controls : list[str] | None
        Liste de colonnes optionnelles (ex: ['part_femmes', 'age_mean', ...]).
        Si None, on tente automatiquement des contrôles “classiques” s’ils existent.
    medals_col : str
        Colonne utilisée comme “médailles last JO” (dans ton notebook c’était total_medailles).

    Returns
    -------
    object
        Résultat statsmodels (RegressionResults).
    """
    dfp = df_model_full.sort_values(["code_sport", "annee"]).copy()

    # log + diff log
    dfp["log_lic"] = np.log1p(dfp["nb_licencies"])
    dfp["dlog_lic"] = dfp.groupby("code_sport")["log_lic"].diff(1)

    # “médailles last JO”
    dfp["med_last"] = dfp[medals_col]

    # filtre période + drop croissance manquante (1ère année de chaque sport)
    dfp = dfp[dfp["annee"].between(start_year, end_year)].copy()
    dfp = dfp.dropna(subset=["dlog_lic"]).copy()

    # contrôles
    if controls is None:
        candidates = ["part_femmes", "age_mean", "nb_departements_actifs"]
        controls = [c for c in candidates if c in dfp.columns]
    else:
        controls = [c for c in controls if c in dfp.columns]

    rhs = "med_last"
    if controls:
        rhs += " + " + " + ".join(controls)

    formula = f"dlog_lic ~ {rhs} + C(code_sport) + C(annee)"
    model = smf.ols(formula, data=dfp).fit(
        cov_type="cluster",
        cov_kwds={"groups": dfp["code_sport"]},
    )

    return model
