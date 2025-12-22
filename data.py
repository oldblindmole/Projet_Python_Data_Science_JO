"""
Module de récupération, nettoyage et fusion des données.

Ce module est conçu pour être importé et utilisé depuis
le notebook principal (main.ipynb).
"""

import os
import unicodedata
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa


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

# --- Données de Licences ---

def reorganiser_colonnes(liste_fichiers=None):
    """
    Réorganise les colonnes de tous les fichiers parquet
    selon l'ordre du premier fichier.

    Paramètres
    ----------
    Aucun

    Retour
    ------
    tables : list[pyarrow.Table].
        Liste des tables avec l'ordre des colonnes harmonisé.
    """
    ref_cols = pq.read_table(liste_fichiers[0]).column_names
    tables = []

    for fichier in liste_fichiers:
        table = pq.read_table(fichier)
        table = table.select(ref_cols)
        tables.append(table)

    return tables

def normalisation_unicode(table):
    """
    Normalise les caractères en unicode dans une table et renvoie un data frame pandas.

    Paramètres
    ----------
    table : pyarrow.Table
        Table à normaliser.

    Retour
    ------
    df : pd.DataFrame
        Dataframe pandas correspondant avec caractères normalisés.
    """
    df = table.to_pandas()

    #normalisation des caractères en unicode pour les variables de fédération
    df["Fédération"] = df["Fédération"].apply(
        lambda x: unicodedata.normalize("NFKC", str(x))
    )

    #règle le problème d'apostrophe sur la fédération française d'hélicoptère
    df["Fédération"] = df["Fédération"].str.replace(
        "Fédération Française d’hélicoptère", "Fédération Française d'hélicoptère", regex=False
    )

    return df

def code_sport(df):
    """
    Ajoute le code_sport à la base des licenciés.

    Paramètres
    ----------
    df : pd.DataFrame
        Data frame pandas auquel ajouter le code sport.

    Retour
    ------
    df : pd.DataFrame
        Dataframe pandas avec la nouvelle colonne code_sport.
    """
    #liste de codes sport dont l'ordre correspond à celui de la liste des fédérations.
    code_list = ["ATH", "AVI", "BAD", "BAK",
             "BOX", "CAK", "CYC", "EQU", 
             "ESC", "FOO", "DIV", "GYM", 
             "HAL", "HAN", "HOC", "JUD", 
             "LUT", "NAT", "PEN", "DIV", 
             "TAE", "TEN", "TDT", "TIR", 
             "TAR", "TRI", "VOI", "VOL", 
             "DIV", "GOL", "RUG", "ESD", 
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
             "RUG", "DIV", "DIV", "DIV", 
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

    #appariement des fédérations avec le bon code sport. 
    fed_list = df["Fédération"].unique()
    cs = pd.DataFrame({
        "Fédération": fed_list,
        "Code_sport": code_list
        })

    #left merge avec la base complète.
    df = df.merge(
        cs,
        left_on = ["Fédération"],
        right_on = ["Fédération"],
        how = "left"
    )

    return df

def code_dep(df, var):
    """
    Crée un code département avec seulement leur numéro.

    Paramètres
    ----------
    df : pd.DataFrame
        Data frame pandas auquel ajouter le code département.
    var : str
        Variable de laquelle extraire le code département.

    Retour
    ------
    df : pd.DataFrame
        Dataframe pandas avec la nouvelle colonne code_dep.
    """
    df["code_dep"] = df[var].str.extract(r"^(\d{2,3}|2A|2B)")

    return df

def renommer_colonnes(df):
    """
    Renomme les colonnes du data frame licenciés.

    Paramètres
    ----------
    df : pd.DataFrame
        Data frame pandas dont les colonnes doivent être renommées.

    Retour
    ------
    df : pd.DataFrame
        Dataframe pandas avec colonnes renommées.
    """
    df.columns = ['code_2024', 'code_annee_n', 'codes_2016_2024', 'federation', 'annee',
       'sexe', 'age', 'tranche_age', 'grande_tranche_age', 'region',
       'departement_long', 'licences_annuelles', 'code_sport','code_dep']

    return df

def gel_licences(df, nom_fichier="data_licences.parquet"):
    """
    Gel de la base des licenciés avec le code sport en fichier parquet.

    Paramètres
    ----------
    df : pd.DataFrame
        Data frame pandas à écrire en parquet.
    nom_fichier : str (optionnel)
        Nom du fichier parquet à écrire.
    """
    chemin_sortie = os.path.join(DATA_DIR, nom_fichier)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, chemin_sortie)

def calcul_ratio_nr_annee(df, annee, var):
    """
    Calcule le ratio de licenciés 'Non Répartis' (NR) par année, pour la variable désirée.

    Paramètres
    ----------
    df : pd.DataFrame
        Le DataFrame pandas contenant les colonnes 'annee', 'code_dep' et 
        'licences_annuelles'.

    Retour
    ------
    np.float(64)
        Le ratio de licenciés non répartis sur une année, pour la variable désirée.
    """
    
    #création d'un data frame sélectionnant les effectifs non répartis 
    if var == "code_dep":
        df_nr = df[df[var].isna()] #les "non répartis" sont inscrits en NaN pour le code département
    else:
        df_nr = df[df[var] == "NR - Non réparti"]

    return df_nr["licences_annuelles"][df_nr["annee"] == annee].sum()/df["licences_annuelles"][df["annee"]==annee].sum()

def calcul_ratio_nr(df, var:str):
    """
    Calcule le ratio de licenciés 'Non Répartis' (NR) dans toute la base, pour la variable désirée.

    Paramètres
    ----------
    df : pd.DataFrame
        Le DataFrame pandas contenant les colonnes 'code_dep' et 
        'licences_annuelles'.
    var : str
        Variable pour laquelle on désire calculer le ratio de non répartition. 

    Retour
    ------
    np.float(64)
        Le ratio de licenciés non répartis dans toute la base, pour la variable désirée.
    """

    #création d'un data frame sélectionnant les effectifs non répartis
    if var == "code_dep":
        df_nr = df[df[var].isna()] #les "non répartis" sont inscrits en NaN pour le code département
    else:
        df_nr = df[df[var] == "NR - Non réparti"] 
    return df_nr["licences_annuelles"].sum()/df["licences_annuelles"].sum()

def tableau_ratios_nr(df, var:str):
    """
    Calcule et présente le ratio de licenciés 'Non Répartis' (NR) par année et le ratio global 
    sous forme de DataFrame Pandas, pour la variable désirée.

    Paramètres
    ----------
    df : pd.DataFrame
        Le DataFrame pandas contenant les colonnes 'annee', 'code_dep' et 
        'licences_annuelles'.
    var : str
        Variable pour laquelle on désire calculer le ratio de non répartition. 


    Retour
    ------
    df_ratios_nr : pd.DataFrame
        Un tableau des ratios formatés en pourcentage.
    """

    resultats = {}
    annees = sorted(df["annee"].unique())

    #calcul du ratio pour chaque année
    for a in annees:
        ratio = calcul_ratio_nr_annee(df, a, var)
        resultats[a] = [f"{ratio * 100:.2f} %"]
   
    #calcul du ratio global
    ratio_global = calcul_ratio_nr(df, var)
    resultats["Global"] = [f"{ratio_global * 100:.2f} %"]

    #création d'un data frame pour présenter proprement les résultats
    df_ratios_nr = pd.DataFrame(resultats, index=[f"Ratio de non répartis {var}"])

    return df_ratios_nr

# --- Données de population départementale ---

def melodi_extraction(url_api):
    """
    Extrait les données de population au niveau départemental pour les années disponibles dans l'API Melodi. 

    Paramètres
    ----------
    url_api : url 
        URL de l'API à interroger.

    Retour
    ------
    data_pop : pd.DataFrame
        Data frame recensant la population départementale pour les années disponibles. 
    """
    get_data = requests.get(url_api, verify=True, timeout = 60)
    data_from_net = get_data.content
    data = json.loads(data_from_net)

    # Extraction des informations du jeu de données
    title = data['title']['fr']
    identifier = data['identifier']

    #Extraction des observations du jeu de données filtré, sur lesquelles on va boucler
    observations = data['observations']
    extracted_data = []

    #Boucle de lecture des observations dans le json
    for obs in observations:
        dimensions = obs['dimensions']

    #Suivant les jeux de données attributes est présent ou non
        if 'attributes' in obs:
            attributes = obs['attributes']
        else:
            attributes = None

    #Suivant les jeux de données value peut être absent
        if 'value' in obs['measures']['OBS_VALUE_NIVEAU']:
            measures = obs['measures']['OBS_VALUE_NIVEAU']['value']
        else:
            measures = None

    #Rassemble tout dans un objet
        if 'attributes' in obs:
            combined_data = {**dimensions,**attributes, 'OBS_VALUE_NIVEAU': measures}
        else:
            combined_data = {**dimensions, 'OBS_VALUE_NIVEAU': measures}

        extracted_data.append(combined_data)

    #Création d'un dataframe pandas
    data_pop = pd.DataFrame(extracted_data)

    print(f'Jeu de données : {identifier} \nTitre : {title} ')

    return data_pop

def clean_population(df):
    """
    Nettoie la base de données de population : 
        Force les types des variables
        Sélectionne les départements cartographiables
        Trie les départements par ordre croissant
    
    Paramètres
    ----------
    df : pd.DataFrame 
        Data frame de population devant être nettoyé. 
    
    Retour
    ------
    data_pop_clean : pd.DataFrame 
        Data frame de population nottoyé. 
    """
    data_pop_clean = df.copy()

    #Conversion de l'année en entier
    data_pop_clean["annee"] = data_pop_clean["annee"].astype(int)

    #Tri des départements
    data_pop_clean = data_pop_clean.sort_values(by="code_dep", ascending=True).reset_index(drop=True)

    #Suppression des départements non cartographiables
    data_pop_clean = data_pop_clean.dropna(subset=["code_dep"]).copy()

    #Conversion du code département en string
    data_pop_clean["code_dep"] = data_pop_clean["code_dep"].astype(str)

    return data_pop_clean

def code_dep_pop(data_pop, var):
    """
    Extrait le code département à partir de la variable de département de la base de population départementale annuelle.
    
    Paramètres 
    ----------
    data_pop : pd.DataFrame
        Data frame de population avec colonnes renommmées. 
    var : str
        Variable à partir de laquelle extraire le code département (extraction des deux derniers caractères)

    Retour
    ------
    data_pop : pd.DataFrame
        Data frame de population annuelle avec la nouvelle variable "code_dep"
    """
    data_pop["code_dep"] = data_pop[var].str.extract(r'(..)$')

    return data_pop

def gel_population(df, nom_fichier="population_dept.csv"):
    """
    Gel de la base de population départementale en CSV.

    Paramètres
    ----------
    df : pd.DataFrame
        Base de données à geler.
    """
    output_path = os.path.join(DATA_DIR, nom_fichier)
    df.to_csv(output_path, index=False)
