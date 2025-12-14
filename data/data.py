"""Fichier de récupération et travail des données"""

import os
import requests
import bs4
import pandas as pd
import numpy as np

# SCRAPING DES DONNÉES

URL_JO = "https://fr.wikipedia.org/wiki/France_aux_Jeux_olympiques"

request_text = requests.get(
    URL_JO,
    headers={"User-Agent": "Python for data science tutorial"},
    timeout=10
).content

page = bs4.BeautifulSoup(request_text, "lxml")


def tableau_scraper(id_html) :
    """ Scrape et renvoie un Data Frame à partir d'un titre dans une page Wikipedia.

    Paramètre
    ---------
    id_html (str) : identifiant html du titre au dessus du tableau dans la page
    
    Output
    ---------
    data_medailles (DataFrame) : tableau scrapé
    """

    titre = page.find(id=id_html)
    table = titre.find_next("table", {"class": "wikitable"})

    table_body = table.find('tbody')
    lignes = table_body.find_all('tr')

    cols = lignes[1].find_all('td')

    dico_medailles = dict()
    for i in range(1,len(lignes)):
        cols = lignes[i].find_all('td')
        cols = [ele.text.strip() for ele in cols]
        # ajout d'index pour éviter que les sports au même classement ne s'écrasent
        cols.insert(0,i)
        dico_medailles[cols[0]] = cols[1:]

    data_medailles = pd.DataFrame.from_dict(dico_medailles,orient='index')

    colonnes_medailles = []
    for ligne in lignes:
        cols = ligne.find_all('th')
        print(cols)
        if len(cols) > 0:
            cols = [ele.get_text(separator=' ').strip().title() for ele in cols]
            colonnes_medailles = cols
    # la 2e colonne comportait des images, elle est désormais vide, sauf pour un total de médailles
    # le titre était autrefois merge avec "Sport"
    # on ajoute donc une cellule vide à la ligne de titre, au niveau de la 2e colonne
    colonnes_medailles.insert(1,"")

    data_medailles.columns = colonnes_medailles[0:]

    # on remplace les cellules vides par NaN (pas d'épreuve)
    data_medailles = data_medailles.replace("", np.nan)
    # on remplace les cellules – par 0 (0 médaille à l'épreuve)
    data_medailles = data_medailles.replace("–", 0)

    return data_medailles


# GEL DES DONNÉES SCRAPÉES

def gel(type_medaille, id_html) :
    """Crée un csv à partir d'un DataFrame issu d'une page Wikipedia.

    Paramètres
    ---------
    type_medaille (str) : couleur de la médaille étudiée
    id_html (str) : identifiant html du titre au dessus du tableau dans la page
    """
    output_path = os.path.join(os.path.dirname(__file__), f"data_{type_medaille}_jo.csv")
    data = tableau_scraper(id_html)
    data.to_csv(output_path, index=False)

a_figer = [["or", "M.C3.A9dailles_d.27or_3"],
           ["argent", "M.C3.A9dailles_d.27argent"],
           ["bronze", "M.C3.A9dailles_de_bronze"]]

for duo in a_figer:
    gel(duo[0], duo[1])


# MERGING ET NOETTOYAGE DE LA BASE DE DONNÉE COMPLÈTE

os.chdir(os.path.dirname(os.path.abspath(__file__)))


data_or = pd.read_csv("data_or_jo.csv")
data_argent = pd.read_csv("data_argent_jo.csv")
data_bronze = pd.read_csv("data_bronze_jo.csv")

diff_medailles = [data_or, data_argent, data_bronze]

# enlever les lignes vides / qui ne nous intéressent pas
for j, medaille in enumerate(diff_medailles):
    diff_medailles[j] = diff_medailles[j].dropna(how="all")
    diff_medailles[j] = diff_medailles[j].drop(diff_medailles[j].tail(3).index)

# enlever les colonnes (années notamment) qui ne nous intéressent pas
diff_medailles = [df[["Sport", "2024", "2020", "2016"]] for df in diff_medailles]

# renommer les variables pour préparer le merge
diff_medailles[0].columns = ["sport", "2024_or", "2020_or", "2016_or"]
diff_medailles[1].columns = ["sport", "2024_argent", "2020_argent", "2016_argent"]
diff_medailles[2].columns = ["sport", "2024_bronze", "2020_bronze", "2016_bronze"]

# merge
data_medailles = diff_medailles[0].merge(diff_medailles[1], on="sport", how="outer")
data_medailles = data_medailles.merge(diff_medailles[2], on="sport", how="outer")

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
             "PEN", "DIV", "RUG",
             "SKT", "SUR", "TAE",
             "TEN", "TDT", "TIR", 
             "TAR", "TRI", "VOI",
             "VOL", "DIV", "EQU"]

data_medailles.insert(0, "code_sport", code_list)

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

# enlever les DIV, qui ne nous intéressent pas
data_medailles = data_medailles[~data_medailles["code_sport"].str.contains("DIV")]

data_medailles.to_csv("data_medailles_jo_2.csv", index=False)

print(data_medailles)
