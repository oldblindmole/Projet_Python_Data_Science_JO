"""Ajout d'une variable Code_sport au data frame"""

import pandas as pd

df = pd.read_parquet("licences_long.parquet")

fed_list = df["Fédération"].unique()
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

# harmonise l'apostrophe entre les deux data frame
cs["Fédération"] = cs["Fédération"].str.replace("’", "'", regex=False)

df = df.merge(
  cs,
  left_on = ["Fédération"],
  right_on = ["Fédération"],
  how = "left"
)
