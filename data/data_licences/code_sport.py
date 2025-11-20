"""Ajout d'une variable Code_sport au data frame"""

import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

os.chdir(os.path.dirname(os.path.abspath(__file__)))

data_licences = pd.read_parquet("licences_long.parquet")

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

# harmonise l'apostrophe entre les deux data frame
cs["Fédération"] = cs["Fédération"].str.replace("’", "'", regex=False)

data_licences = data_licences.merge(
  cs,
  left_on = ["Fédération"],
  right_on = ["Fédération"],
  how = "left"
)

table = pa.Table.from_pandas(data_licences)
pq.write_table(table, "data_licences.parquet")
