"""Fichier de définition de la fonction tableau_scraper"""

import requests
import bs4
import pandas as pd
import numpy as np


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

    # on remplace les cellules vides par NaN
    data_medailles = data_medailles.replace("", np.nan)
    data_medailles = data_medailles.replace("–", 0)

    return data_medailles
