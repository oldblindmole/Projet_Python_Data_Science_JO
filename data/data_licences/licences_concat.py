"""Création de la base de données au format long"""

import pyarrow.parquet as pq
import pyarrow as pa

liste_fichiers = ["Lics_2016_semidef.parquet",
                  "Lics_2017_semidef.parquet", 
                  "Lics_2018_semidef.parquet",
                  "Lics_2019_def.parquet",
                  "Lics_2020_def.parquet",
                  "Lics_2021_def.parquet",
                  "Lics_2022_def.parquet",
                  "Lics_2023_semidef.parquet",
                  "Lics_2024_semidef.parquet"]

# réorganiser les colonnes pour qu'elles soient dans le même ordre dans tous les fichiers
ref_cols = pq.read_table(liste_fichiers[0]).column_names
tables = []
for fichier in liste_fichiers:
    table = pq.read_table(fichier)
    table = table.select(ref_cols)
    tables.append(table)

# concaténer les tables et enregistrer
concat = pa.concat_tables(tables)
pq.write_table(concat, "licences_long.parquet")