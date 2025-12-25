# Projet Python pour la Data Science : Influence des médailles remportées par la France aux Jeux Olympiques sur le nombre le licenciés sportifs en France.

_Autrices : Melissa MIGAN; Camille PEYTHIEUX-TALDIR, Romane PLUQUET_

## Sujet : Influence des médailles remportées par la France aux Jeux Olympiques sur le nombre le licenciés sportifs en France.
**Titre** #TODO présenter en quelques lignes le sujet

**Problématique** : Le fait de remporter des médailles olympiques incite-t-il à la pratique sportive ? 

## Données
Nous exploitons des données de trois types :  
- nombre et couleur des médailles gagnées aux JO entre 2016 et 2024,
- effectifs de licenciés sportifs en France entre 2016 et 2024,
- population départementale (municipale) entre 2016 et 2023,

Ces données sont obtenues par trois sources différentes, respectivement :
- webscraping de la page Wikipédia "France aux Jeux Olympiques",
- fichiers CSV fournis par l'Injep (Institut national de la jeunesse et de l'éducation populaire), importés au format parquet car trop lourds pour un repo git, 
- requête via l'API Melodi de l'INSEE. 

La base de données de l'INJEP a l'avantage de présenter une classfication fine des licenciés selon leurs caractéristiques socio-démographiques (âge, sexe, département d'exercice).

## Modèle
#TODO raconter quels modèles on a utilisé tout ça

## Reproduction du projet

Installation

Cloner le projet.
Installer les dépendances : pip install -r requirements.txt

#TODO décrit comment bien lancer
* pip install -r requirements.txt
* ouvrir main.ipynb
* Restart & Run All (préciser qu’il faut avoir les fichiers de données / où ils sont)

## Structure du repo
- dossier `data` : dossier contenant les données brutes et receptionnant les données nettoyées, 
- dossier `jo`: package permettant la visualisation interactive des données,
- dossier `traitement`: package permettant la récupération et le nettoyage des données, 
- fichier main.ipynb : rapport analytique, 
