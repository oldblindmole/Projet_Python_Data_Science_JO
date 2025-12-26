"""
Package de préparation des données : pipes de récupération et de nettoyage

Les données brutes sont prises ou inscrites dans 'data/data_brut', 
et les données propres sont enregistrées dans le dossier 'data/data_clean'. 

Ce package est conçu pour être utilisé dans le notebook final (main.ipynb).
"""

from pathlib import Path

# --- Paramètres globaux ---
URL_JO = "https://fr.wikipedia.org/wiki/France_aux_Jeux_olympiques"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
CACHE_DIR = DATA_DIR
