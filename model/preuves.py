"""
Docstring for model.preuves
"""

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score, mean_absolute_error
import statsmodels.formula.api as smf


###### preuve 1 ############
def eval_ablation_models(df_model_full, train_years=(2017, 2020), test_years=(2022, 2024),
                         medals_col="total_medailles", alpha=1.0):
    df = df_model_full.sort_values(["code_sport","annee"]).copy()
    df["y"] = np.log1p(df["nb_licencies"])
    df["log_lag1"] = df.groupby("code_sport")["y"].shift(1)
    df["trend"] = df["annee"] - df["annee"].min()

    # médaille "last JO" : dans ton dataset, total_medailles est déjà last JO (via jo_ref)
    df["med_last"] = df[medals_col]

    df = df.dropna(subset=["log_lag1"]).copy()

    train_df = df[df["annee"].between(*train_years)].copy()
    test_df  = df[df["annee"].between(*test_years)].copy()

    def fit_predict(feats):
        Xtr = pd.get_dummies(train_df[["code_sport"] + feats], columns=["code_sport"], drop_first=True).astype("float32")
        Xte = pd.get_dummies(test_df[["code_sport"] + feats], columns=["code_sport"], drop_first=True).astype("float32")
        Xte = Xte.reindex(columns=Xtr.columns, fill_value=0).astype("float32")

        ytr = train_df["y"].astype("float32").values
        yte = test_df["y"].astype("float32").values

        m = Ridge(alpha=alpha)
        m.fit(Xtr, ytr)
        pred = m.predict(Xte)

        return {
            "R2_log": float(r2_score(yte, pred)),
            "MAE_niveau": float(mean_absolute_error(np.expm1(yte), np.expm1(pred)))
        }

    res = []
    res.append({"model":"M0: inertie seule (lag1)", **fit_predict(["log_lag1"])})
    res.append({"model":"M1: médailles seules", **fit_predict(["med_last"])})
    res.append({"model":"M2: inertie + médailles", **fit_predict(["log_lag1","med_last"])})
    res = pd.DataFrame(res)
    return res

### a mettre dans le main###
ablation = eval_ablation_models(df_model_full)
ablation


######## preuve 2 ########


def ridge_coefficients(df_model_full, train_years=(2017, 2020), medals_col="total_medailles", alpha=1.0):
    df = df_model_full.sort_values(["code_sport","annee"]).copy()
    df["y"] = np.log1p(df["nb_licencies"])
    df["log_lag1"] = df.groupby("code_sport")["y"].shift(1)
    df["trend"] = df["annee"] - df["annee"].min()
    df["med_last"] = df[medals_col]
    df = df.dropna(subset=["log_lag1"]).copy()

    train_df = df[df["annee"].between(*train_years)].copy()
    feats = ["log_lag1", "trend", "med_last"]

    X = pd.get_dummies(train_df[["code_sport"] + feats], columns=["code_sport"], drop_first=True).astype("float32")
    y = train_df["y"].astype("float32").values

    m = Ridge(alpha=alpha)
    m.fit(X, y)

    coef = pd.Series(m.coef_, index=X.columns).sort_values(key=lambda s: s.abs(), ascending=False)
    return coef

### a mettre dans le main ####
coef = ridge_coefficients(df_model_full)
coef.head(15)


######## preuve 3 #######

def test_medals_incremental(df_model_full, medals_col="total_medailles", start=2017, end=2024):
    dfp = df_model_full.sort_values(["code_sport","annee"]).copy()
    dfp["log_lic"] = np.log1p(dfp["nb_licencies"])
    dfp["dlog_lic"] = dfp.groupby("code_sport")["log_lic"].diff(1)
    dfp = dfp[dfp["annee"].between(start, end)].dropna(subset=["dlog_lic"]).copy()

    dfp["med_last"] = dfp[medals_col]

    # contrôles optionnels (si tu veux)
    controls = [c for c in ["part_femmes","age_mean","nb_departements_actifs"] if c in dfp.columns]
    ctrl = (" + " + " + ".join(controls)) if controls else ""

    f0 = f"dlog_lic ~ C(code_sport) + C(annee){ctrl}"
    f1 = f"dlog_lic ~ med_last + C(code_sport) + C(annee){ctrl}"

    m0 = smf.ols(f0, data=dfp).fit(cov_type="cluster", cov_kwds={"groups": dfp["code_sport"]})
    m1 = smf.ols(f1, data=dfp).fit(cov_type="cluster", cov_kwds={"groups": dfp["code_sport"]})

    # Wald test: H0: med_last = 0
    wald = m1.wald_test("med_last = 0")
    return m0, m1, wald

#### a mettre dans le main####
m0, m1, wald = test_medals_incremental(df_model_full)
print(wald)
print("beta medals:", m1.params.get("med_last", np.nan))

