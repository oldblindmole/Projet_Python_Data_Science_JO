"""Fichier de création d'une table "médailles" complète et nettoyée"""

import pandas as pd
import numpy as np

data_or = pd.read_csv("data_or.csv")
data_argent = pd.read_csv("data_argent.csv")
data_bronze = pd.read_csv("data_bronze.csv")

diff_medailles = [data_or, data_argent, data_bronze]

for i in range(len(diff_medailles)):
    diff_medailles[i] = diff_medailles[i].dropna(how="all")
    diff_medailles[i] = diff_medailles[i].drop(diff_medailles[i].tail(3).index)
    diff_medailles = [df[["Sport", "2024", "2020"]] for df in diff_medailles]

diff_medailles[0].columns = ["Sport", "2024_or", "2020_or"]
diff_medailles[1].columns = ["Sport", "2024_argent", "2020_argent"]
diff_medailles[2].columns = ["Sport", "2024_bronze", "2020_bronze"]

data_medailles = diff_medailles[0].merge(diff_medailles[1], on="Sport", how="outer")
data_medailles = data_medailles.merge(diff_medailles[2], on="Sport", how="outer")

print(data_medailles)

# enlever les lignes vides

# enlever les années qui ne nous intéressent pas

# merge par sport