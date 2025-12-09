""" Fichier de merge des bases de données pour en obtenir une complète"""

import os
import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))

df_lic = pd.read_parquet("data_licences/data_licences.parquet")
df_med = pd.read_csv("data_medailles/data_medailles_jo.csv")

df_complet = pd.merge(df_lic, df_med, how='left', on="code_sport")

df_complet.to_csv("data_complet.csv")
