"""Ajout d'une variable code_sport au data frame"""

import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import unicodedata

os.chdir(os.path.dirname(os.path.abspath(__file__)))

data_licences = pd.read_parquet("licences_long.parquet")

#normalisation des caractères en Unicode
data_licences["Fédération"] = data_licences["Fédération"].apply(
    lambda x: unicodedata.normalize("NFKC", str(x))
)

fed_list = data_licences["Fédération"].unique()
code_list = ["ATH", "AVI", "BAD", "BAK",
             "BOX", "CAK", "CYC", "EQU", 
             "ESC", "FOO", "DIV", "GYM", 
             "HAL", "HAN", "HOC", "JUD", 
             "LUT", "NAT", "PEN", "DIV", 
             "TAE", "TEN", "TDT", "TIR", 
             "TAR", "TRI", "VOI", "VOL", 
             "DIV", "GOL", "DIV", "ESD", 
             "SKT", "SUR", "BAS", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "KAR", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV", "DIV", "DIV", 
             "DIV", "DIV"]

cs = pd.DataFrame({
    "Fédération": fed_list,
    "Code_sport": code_list
    })

data_licences = data_licences.merge(
  cs,
  left_on = ["Fédération"],
  right_on = ["Fédération"],
  how = "left"
)

#création d'un code département pour n'avoir que leur numéro
data_licences["code_dep"] = data_licences["Département"].str.extract(r"^(\d{2,3}|2A|2B)")

#renomme les colonnes
data_licences.columns = ['code_2024', 'code_année_n', 'codes_2016_2024', 'federation', 'annee',
       'sexe', 'age', 'tranche_age', 'grande_tranche_age', 'region',
       'departement_long', 'licences_annuelles', 'code_sport','code_dep']

#gel des données
table = pa.Table.from_pandas(data_licences)
pq.write_table(table, "data_licences.parquet")
