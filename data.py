"""
Module de récupération, nettoyage et fusion des données.

Ce module est conçu pour être importé et utilisé depuis
le notebook principal (main.ipynb).
"""

import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


# Paramètres globaux

URL_JO = "https://fr.wikipedia.org/wiki/France_aux_Jeux_olympiques"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = BASE_DIR


# Chargement de la page Wikipedia

def charger_page_wiki(url=URL_JO):
    """
    Télécharge et analyse une page Wikipedia.

    Paramètres
    ----------
    url : str, optionnel
        URL de la page à scraper.

    Retour
    ------
    soup : BeautifulSoup
        Objet BeautifulSoup contenant le HTML parsé.
    """
    response = requests.get(
        url,
        headers={"User-Agent": "Python data science project"},
        timeout=10
    )
    response.raise_for_status()

    return BeautifulSoup(response.content, "lxml")


# Scraping des tableaux des médailles

def tableau_scraper(soup, id_html):
    """
    Scrape un tableau de médailles à partir d'un identifiant HTML Wikipedia.

    Paramètres
    ----------
    soup : BeautifulSoup
        Page Wikipedia parsée.
    id_html : str
        Identifiant HTML du titre situé au-dessus du tableau.

    Retour
    ------
    data_medailles : pandas.DataFrame
        Tableau contenant les médailles par sport et par année.
    """
    titre = soup.find(id=id_html)
    table = titre.find_next("table", {"class": "wikitable"})

    lignes = table.find("tbody").find_all("tr")

    data_dict = {}

    for i in range(1, len(lignes)):
        cols = lignes[i].find_all("td")
        cols = [ele.text.strip() for ele in cols]
        cols.insert(0, i)  # index artificiel
        data_dict[i] = cols[1:]

    data_medailles = pd.DataFrame.from_dict(data_dict, orient="index")

    # Récupération des noms de colonnes
    colonnes = []
    for ligne in lignes:
        th = ligne.find_all("th")
        if th:
            colonnes = [ele.get_text(separator=" ").strip().title() for ele in th]

    # Colonne vide (images supprimées dans Wikipedia)
    colonnes.insert(1, "")
    data_medailles.columns = colonnes

    # Nettoyage
    data_medailles = data_medailles.replace("", np.nan)
    data_medailles = data_medailles.replace("–", 0)

    return data_medailles


# Sauvegarde (gel) des données scrapées

def gel_tableau_medailles(type_medaille, id_html):
    """
    Scrape un tableau de médailles et le sauvegarde en CSV.

    Paramètres
    ----------
    type_medaille : str
        Type de médaille ('or', 'argent', 'bronze').
    id_html : str
        Identifiant HTML du tableau à scraper.
    """
    soup = charger_page_wiki()
    data = tableau_scraper(soup, id_html)

    output_path = os.path.join(DATA_DIR, f"data_{type_medaille}_jo.csv")
    data.to_csv(output_path, index=False)


# Fusion et nettoyage de la base finale

def fusion_nettoyage_base():
    """
    Charge, nettoie et fusionne les données de médailles
    (or, argent, bronze) pour produire une base finale.

    Retour
    ------
    data_medailles : pandas.DataFrame
        Base de données propre avec totaux par sport et par année.
    """
    data_or = pd.read_csv(os.path.join(DATA_DIR, "data_or_jo.csv"))
    data_argent = pd.read_csv(os.path.join(DATA_DIR, "data_argent_jo.csv"))
    data_bronze = pd.read_csv(os.path.join(DATA_DIR, "data_bronze_jo.csv"))

    diff_medailles = [data_or, data_argent, data_bronze]

    # Nettoyage des lignes inutiles
    for i, df in enumerate(diff_medailles):
        df = df.dropna(how="all")
        df = df.drop(df.tail(3).index)
        diff_medailles[i] = df[["Sport", "2024", "2020", "2016"]]

    # Renommage des colonnes
    diff_medailles[0].columns = ["sport", "2024_or", "2020_or", "2016_or"]
    diff_medailles[1].columns = ["sport", "2024_argent", "2020_argent", "2016_argent"]
    diff_medailles[2].columns = ["sport", "2024_bronze", "2020_bronze", "2016_bronze"]

    # Fusion des bases
    data_medailles = diff_medailles[0].merge(diff_medailles[1], on="sport", how="outer")
    data_medailles = data_medailles.merge(diff_medailles[2], on="sport", how="outer")

    # Suppression des sports sans épreuves
    cols_medailles = data_medailles.columns[1:]
    data_medailles = data_medailles.dropna(subset=cols_medailles, how="all")

    # Ajout des codes sport
    code_list = [
        "ATH", "AVI", "BAD", "BAK", "BOX", "DIV", "CAK", "CYC", "ESD",
        "ESC", "FOO", "GOL", "GYM", "HAL", "HAN", "HOC", "JUD", "KAR",
        "LUT", "NAT", "DIV", "PEN", "DIV", "RUG", "SKT", "SUR", "TAE",
        "TEN", "TDT", "TIR", "TAR", "TRI", "VOI", "VOL", "DIV", "EQU"
    ]

    data_medailles.insert(0, "code_sport", code_list)

    # Calcul des totaux de médailles
    for year in ["2016", "2020", "2024"]:
        data_medailles[f"total_medailles_{year}"] = (
            data_medailles[f"{year}_or"].fillna(0)
            + data_medailles[f"{year}_argent"].fillna(0)
            + data_medailles[f"{year}_bronze"].fillna(0)
        )

    # Suppression des sports divers (DIV)
    data_medailles = data_medailles[~data_medailles["code_sport"].str.contains("DIV")]

    return data_medailles


# Sauvegarde de la base finale

def gel_base_medailles_finale(filename="data_medailles_jo.csv"):
    """
    Construit et sauvegarde la base finale de médailles.

    Paramètres
    ----------
    filename : str, optionnel
        Nom du fichier CSV de sortie.

    Retour
    ------
    data_medailles : pandas.DataFrame
        Base finale sauvegardée.
    """
    data = fusion_nettoyage_base()
    output_path = os.path.join(DATA_DIR, filename)
    data.to_csv(output_path, index=False)

    return data
