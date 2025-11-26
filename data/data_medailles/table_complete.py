"""Fichier de création d'une table "médailles" complète et nettoyée"""

import os
import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))


data_or = pd.read_csv("data_or.csv")
data_argent = pd.read_csv("data_argent.csv")
data_bronze = pd.read_csv("data_bronze.csv")

diff_medailles = [data_or, data_argent, data_bronze]

# enlever les lignes vides / qui ne nous intéressent pas
for i, medaille in enumerate(diff_medailles):
    diff_medailles[i] = diff_medailles[i].dropna(how="all")
    diff_medailles[i] = diff_medailles[i].drop(diff_medailles[i].tail(3).index)

# enlever les colonnes (années notamment) qui ne nous intéressent pas
diff_medailles = [df[["Sport", "2024", "2020", "2016"]] for df in diff_medailles]

# renommer les variables pour préparer le merge
diff_medailles[0].columns = ["Sport", "2024_or", "2020_or", "2016_or"]
diff_medailles[1].columns = ["Sport", "2024_argent", "2020_argent", "2016_argent"]
diff_medailles[2].columns = ["Sport", "2024_bronze", "2020_bronze", "2016_bronze"]

# merge
data_medailles = diff_medailles[0].merge(diff_medailles[1], on="Sport", how="outer")
data_medailles = data_medailles.merge(diff_medailles[2], on="Sport", how="outer")

# enlever les lignes avec NaN dans les colonnes médailles
# (pas d'épreuves dans les 2 années)
cols_to_check = data_medailles.columns[1:7]
data_medailles = data_medailles.dropna(subset=cols_to_check, how="all")

# ajout du code sport
code_list = ["ATH", "AVI", "BAD",
             "BAK", "BOX", "DIV",
             "CAK", "CYC", "ESD", 
             "ESC", "FOO", "GOL",
             "GYM", "HAL", "HAN",
             "HOC", "JUD", "KAR",
             "LUT", "NAT", "DIV",
             "PEN", "DIV", "DIV",
             "SKT", "SUR", "TAE",
             "TEN", "TDT", "TIR", 
             "TAR", "TRI", "VOI",
             "VOL", "DIV", "EQU"]

data_medailles.insert(0, "Code_sport", code_list)

# ajout total des médailles
data_medailles["total_medailles_2016"] = (
    data_medailles["2016_or"].fillna(0)
    + data_medailles["2016_argent"].fillna(0)
    + data_medailles["2016_bronze"].fillna(0)
)

data_medailles["total_medailles_2020"] = (
    data_medailles["2020_or"].fillna(0)
    + data_medailles["2020_argent"].fillna(0)
    + data_medailles["2020_bronze"].fillna(0)
)

data_medailles["total_medailles_2024"] = (
    data_medailles["2024_or"].fillna(0)
    + data_medailles["2024_argent"].fillna(0)
    + data_medailles["2024_bronze"].fillna(0)
)


data_medailles.to_csv("data_medailles.csv")

print(data_medailles)
