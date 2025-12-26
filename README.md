# Projet Python pour la Data Science

_Autrices : Melissa MIGAN; Camille PEYTHIEUX-TALDIR, Romane PLUQUET (2025)._

## Sujet : Influence des médailles remportées par la France aux Jeux Olympiques sur le nombre le licenciés sportifs en France.
Les Jeux Olympiques de Paris 2024 ont été présentés comme une réussite artistique, économique et sportive. Les athlètes français ont été largement médiatisés, et ont remporté un nombre important de médailles. Ces Jeux ont également été l'occasion d'une forte médiatisation de sports jusque là relativement méconnus en France, comme tennis de table grâce aux performances des frères Lebrun. Dans ce cas, la réussite sportive aux Jeux Olympiques affecte-t-elle la pratique sportive de la population française ? Nous mesurerons ici la réussite sportive aux Jeux grâce aux médailles olympiques remportées, et la pratique sportive de la population grâce au nombre de licenciés sportifs.

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
**Installation**. Cloner le projet.
Installer les dépendances Python à l’aide de la commande :
`pip install -r requirements.txt`

**Lancement**. Ouvrir le notebook `main.ipynb`, qui constitue le rapport analytique principal du projet.

## Structure du repo
- dossier `data` : dossier contenant les données brutes et receptionnant les données nettoyées, 
- dossier `jo`: package permettant la visualisation interactive des données,
- dossier `traitement`: package permettant la récupération et le nettoyage des données, 
- fichier `main.ipynb` : notebook principal servant de rapport analytique et de support de restitution.
