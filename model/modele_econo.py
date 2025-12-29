# """
# Estimation économétrique de l'effet des médailles sur la croissance des licenciés.
# """

# from __future__ import annotations

# import numpy as np
# import pandas as pd
# import statsmodels.formula.api as smf


# def fit_modele_croissance(
#     df_model_full: pd.DataFrame,
#     start_year: int = 2017,
#     end_year: int = 2024,
#     controls: list[str] | None = None,
#     medals_col: str = "total_medailles",
# ) -> object:
#     """
#     Estime un modèle de croissance (différence de log) avec effets fixes sport et année,
#     et erreurs standards clusterisées au niveau du sport.

#     Modèle :
#         dlog_lic ~ med_last + controls + FE_sport + FE_annee

#     Parameters
#     ----------
#     df_model_full : pd.DataFrame
#         Dataset final contenant au moins :
#         ['code_sport', 'annee', 'licences_annuelles', medals_col]
#     start_year : int
#     end_year : int
#     controls : list[str] | None
#         Liste de colonnes optionnelles (ex: ['part_femmes', 'age_mean', ...]).
#         Si None, on tente automatiquement des contrôles “classiques” s’ils existent.
#     medals_col : str
#         Colonne utilisée comme “médailles last JO” (dans ton notebook c’était total_medailles).

#     Returns
#     -------
#     object
#         Résultat statsmodels (RegressionResults).
#     """
#     # Tri et copie défensive
#     dfp = df_model_full.sort_values(["code_sport", "annee"]).copy()

#     # Construction de la variable dépendante : diff du log(1 + licences_annuelles)
#     dfp["log_lic"] = np.log1p(dfp["licences_annuelles"])
#     dfp["dlog_lic"] = dfp.groupby("code_sport")["log_lic"].diff(1)

#     # Variable explicative principale : médailles associées aux JO de référence
#     dfp["med_last"] = dfp[medals_col]

#     # Filtrage de la période et suppression des dlog manquants (première année par sport)
#     dfp = dfp[dfp["annee"].between(start_year, end_year)].copy()
#     dfp = dfp.dropna(subset=["dlog_lic"]).copy()

#     # Sélection des contrôles
#     if controls is None:
#         candidates = ["part_femmes", "age_mean", "nb_departements_actifs"]
#         controls = [c for c in candidates if c in dfp.columns]
#     else:
#         controls = [c for c in controls if c in dfp.columns]

#     # Construction du terme de droite (RHS) de la formule
#     rhs = "med_last"
#     if controls:
#         rhs += " + " + " + ".join(controls)

#     # Effets fixes sport + année via variables catégorielles
#     formula = f"dlog_lic ~ {rhs} + C(code_sport) + C(annee)"

#     # Estimation OLS avec erreurs clusterisées au niveau du sport
#     model = smf.ols(formula, data=dfp).fit(
#         cov_type="cluster",
#         cov_kwds={"groups": dfp["code_sport"]},
#     )

# return model



# """
# Estimation économétrique de l'effet des médailles sur la croissance des licences (panel sport-année).

# Spécification (avec effets fixes) :
#     dlog_lic_{s,t} = beta * med_last_{s,t} + X_{s,t}'gamma + alpha_s + tau_t + eps_{s,t}

# - alpha_s : effets fixes sport (C(code_sport))
# - tau_t   : effets fixes année (C(annee))
# - SE : clusterisées par sport si possible (sinon robustes HC1 si trop peu de sports)
# """

# from __future__ import annotations

# import numpy as np
# import pandas as pd
# import statsmodels.formula.api as smf


# def fit_modele_croissance(
#     df_model_full: pd.DataFrame,
#     start_year: int = 2017,
#     end_year: int = 2024,
#     medals_col: str = "total_medailles",
#     controls: list[str] | None = None,
#     auto_controls: bool = False,
#     min_clusters_for_cluster_se: int = 15,
# ):
#     """
#     Estime un modèle de croissance (différence de log) avec effets fixes sport et année.

#     Paramètres
#     ----------
#     df_model_full : pd.DataFrame
#         Doit contenir : ['code_sport', 'annee', 'licences_annuelles', medals_col]
#     start_year, end_year : int
#         Fenêtre d'estimation (inclusive).
#     medals_col : str
#         Colonne des médailles (associées au JO de référence).
#     controls : list[str] | None
#         Contrôles à inclure (ex: ['part_femmes', 'age_mean', 'nb_departements_actifs']).
#         Si None, aucun contrôle n'est inclus (sauf si auto_controls=True).
#     auto_controls : bool
#         Si True et controls=None, tente d'ajouter automatiquement des contrôles "classiques" s'ils existent.
#     min_clusters_for_cluster_se : int
#         En-dessous de ce nombre de sports, on évite les SE cluster (souvent instables) et on passe en HC1.

#     Retour
#     ------
#     RegressionResults (statsmodels)
#     """
#     dfp = df_model_full.sort_values(["code_sport", "annee"]).copy()

#     # Vérifs colonnes minimales
#     required = ["code_sport", "annee", "licences_annuelles", medals_col]
#     missing = [c for c in required if c not in dfp.columns]
#     if missing:
#         raise ValueError(f"Colonnes manquantes : {missing}")

#     # Variable dépendante : dlog(1 + licences)
#     lic = pd.to_numeric(dfp["licences_annuelles"], errors="coerce")
#     dfp["log_lic"] = np.log1p(lic.clip(lower=0).fillna(0.0))
#     dfp["dlog_lic"] = dfp.groupby("code_sport")["log_lic"].diff(1)

#     # Variable médailles
#     dfp["med_last"] = pd.to_numeric(dfp[medals_col], errors="coerce").fillna(0.0)

#     # Filtre période + drop 1ère année par sport
#     dfp = dfp[dfp["annee"].between(start_year, end_year)].dropna(subset=["dlog_lic"]).copy()

#     # Choix contrôles
#     if controls is None and auto_controls:
#         candidates = ["part_femmes", "age_mean", "nb_departements_actifs"]
#         controls = [c for c in candidates if c in dfp.columns]
#     elif controls is None:
#         controls = []
#     else:
#         controls = [c for c in controls if c in dfp.columns]

#     # Nettoyage contrôles + filtrage colinéarité avec FE
#     kept_controls = []
#     for c in controls:
#         x = pd.to_numeric(dfp[c], errors="coerce")
#         # si contrôle quasi vide
#         if x.notna().sum() < 10:
#             continue
#         dfp[c] = x

#         # Si le contrôle est constant dans (presque) tous les sports -> absorbé par FE sport
#         nun_sport = dfp.groupby("code_sport")[c].nunique(dropna=True)
#         absorbed_by_sport = (nun_sport <= 1).mean() > 0.9

#         # Si le contrôle est constant dans (presque) toutes les années -> absorbé par FE année
#         nun_year = dfp.groupby("annee")[c].nunique(dropna=True)
#         absorbed_by_year = (nun_year <= 1).mean() > 0.9

#         if absorbed_by_sport or absorbed_by_year:
#             continue

#         kept_controls.append(c)

#     # Formule
#     rhs = "med_last"
#     if kept_controls:
#         rhs += " + " + " + ".join(kept_controls)

#     formula = f"dlog_lic ~ {rhs} + C(code_sport) + C(annee)"

#     # SE : cluster si assez de sports, sinon HC1 (plus stable quand peu de clusters)
#     n_clusters = dfp["code_sport"].nunique()
#     if n_clusters >= min_clusters_for_cluster_se:
#         model = smf.ols(formula, data=dfp).fit(
#             cov_type="cluster",
#             cov_kwds={"groups": dfp["code_sport"]},
#         )
#     else:
#         model = smf.ols(formula, data=dfp).fit(cov_type="HC1")

#     # (optionnel) infos utiles
#     model._meta = {
#         "n_obs": int(dfp.shape[0]),
#         "n_sports": int(n_clusters),
#         "controls_kept": kept_controls,
#         "se_type": "cluster" if n_clusters >= min_clusters_for_cluster_se else "HC1",
#     }
#     return model



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
