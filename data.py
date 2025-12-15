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

# --- Paramètres globaux ---
URL_JO = "https://fr.wikipedia.org/wiki/France_aux_Jeux_olympiques"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = BASE_DIR


# --- Chargement de la page Wikipedia ---
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


# --- Scraping des tableaux de médailles ---
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

    # Colonnes du tableau
    colonnes = []
    for ligne in lignes:
        th = ligne.find_all("th")
        if th:
            colonnes = [ele.get_text(separator=" ").strip().title() for ele in th]

    colonnes.insert(1, "")  # colonne vide pour les images supprimées
    data_medailles.columns = colonnes

    # Nettoyage des valeurs
    data_medailles = data_medailles.replace("", np.nan)
    data_medailles = data_medailles.replace("–", 0)

    return data_medailles


# --- Sauvegarde d'un tableau de médailles ---
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


# --- Nettoyage d'une base de médailles ---
def nettoyer_base(df):
    """
    Nettoie un DataFrame de médailles en supprimant les lignes et colonnes inutiles.

    Paramètres
    ----------
    df : pandas.DataFrame
        DataFrame à nettoyer.

    Retour
    ------
    df_nettoyee : pandas.DataFrame
        DataFrame nettoyé.
    """
    df = df.dropna(how="all")
    df = df.drop(df.tail(3).index)
    df = df[["Sport", "2024", "2020", "2016"]]
    return df


# --- Fusion des bases de médailles ---
def fusionner_bases(data_or, data_argent, data_bronze):
    """
    Fusionne les bases de médailles or, argent et bronze.

    Paramètres
    ----------
    data_or, data_argent, data_bronze : pandas.DataFrame
        DataFrames de médailles par type.

    Retour
    ------
    data_medailles : pandas.DataFrame
        Base fusionnée et nettoyée.
    """
    # Nettoyage
    data_or = nettoyer_base(data_or)
    data_argent = nettoyer_base(data_argent)
    data_bronze = nettoyer_base(data_bronze)

    # Renommage des colonnes
    data_or.columns = ["sport", "2024_or", "2020_or", "2016_or"]
    data_argent.columns = ["sport", "2024_argent", "2020_argent", "2016_argent"]
    data_bronze.columns = ["sport", "2024_bronze", "2020_bronze", "2016_bronze"]

    # Fusion
    data_medailles = data_or.merge(data_argent, on="sport", how="outer")
    data_medailles = data_medailles.merge(data_bronze, on="sport", how="outer")

    # Supprimer les sports sans épreuves
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

    # Totaux par année
    for year in ["2016", "2020", "2024"]:
        data_medailles[f"total_medailles_{year}"] = (
            data_medailles[f"{year}_or"].fillna(0)
            + data_medailles[f"{year}_argent"].fillna(0)
            + data_medailles[f"{year}_bronze"].fillna(0)
        )

    # Supprimer les sports divers (DIV)
    data_medailles = data_medailles[~data_medailles["code_sport"].str.contains("DIV")]

    return data_medailles


# --- Construction et sauvegarde de la base finale ---
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
    data_or = pd.read_csv(os.path.join(DATA_DIR, "data_or_jo.csv"))
    data_argent = pd.read_csv(os.path.join(DATA_DIR, "data_argent_jo.csv"))
    data_bronze = pd.read_csv(os.path.join(DATA_DIR, "data_bronze_jo.csv"))

    data_medailles = fusionner_bases(data_or, data_argent, data_bronze)
    output_path = os.path.join(DATA_DIR, filename)
    data_medailles.to_csv(output_path, index=False)

    return data_medailles



""" Fichier de merge des bases de données pour en obtenir une complète"""

import os
import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))

df_lic = pd.read_parquet("data/data_licences/data_licences.parquet")
df_med = pd.read_csv("data/data_medailles/data_medailles_jo.csv")

df_complet = pd.merge(df_lic, df_med, how='left', on="code_sport")

df_complet.to_parquet("data_complet.parquet")
