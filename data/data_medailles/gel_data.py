"""Fichier permettant de figer les bases de données issues de Wikipedia"""

import os
from fonction_tableau_scraper import tableau_scraper

def gel(type_medaille, id_html) :
    """Crée un csv à partir d'un DataFrame issu d'une page Wikipedia.

    Paramètres
    ---------
    type_medaille (str) : couleur de la médaille étudiée
    id_html (str) : identifiant html du titre au dessus du tableau dans la page
    """
    output_path = os.path.join(os.path.dirname(__file__), f"data_{type_medaille}.csv")
    data = tableau_scraper(id_html)
    data.to_csv(output_path, index=False)

a_figer = [["or", "M.C3.A9dailles_d.27or_3"],
           ["argent", "M.C3.A9dailles_d.27argent"],
           ["bronze", "M.C3.A9dailles_de_bronze"]]

for duo in a_figer:
    gel(duo[0], duo[1])
