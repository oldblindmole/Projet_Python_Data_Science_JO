# Projet Python pour la Data Science

_Autrices : Melissa MIGAN; Camille PEYTHIEUX-TALDIR, Romane PLUQUET (2025)._

## Sujet : Influence des médailles remportées par la France aux Jeux Olympiques sur le nombre le licenciés sportifs en France.
Les Jeux Olympiques de 2024 se sont tenus du 26 juillet au 11 août 2024 à Paris, en Seine-Saint-Denis, à Marseille et à Tahiti. Ils ont été présentés comme une réussite sportive pour la France : la délégation française y a obtenu son record de médailles depuis 1900, et l'athlète le plus titré des Jeux de Paris est le nageur français Léon Marchand (4 médailles d'or). De plus, cette édition a permis de mettre en lumière et de médiatiser des sports plus méconnus en France, comme le tennis de table grâce aux frères Lebrun. 

Le 23 juin 2023, le Président de la République française Emmanuel Macron s'exprimait sur France Télévision. Il y développait l'idée suivante : "_En matière sportive, qu’est-ce que ça veut dire ? Grâce à nos JO, [...] accompagner nos clubs, nos associations, évidemment, toutes les fédérations sportives qui structurent le sport en France[...]. L'objectif de tout ça, c'est faire quoi ? De la France, cette nation sportive. De plus en plus de jeunes et d'adultes pratiqueront le sport en vue des J.O._" Emmanuel Macron insiste alors sur l'idée suivante : les Jeux Olympiques de Paris 2024 auraient également pour but de dynamiser la pratique sportive en France.

**Problématique** : Le fait de remporter des médailles olympiques incite-t-il à la pratique sportive ? 

Ici, nous considèrerons que le nombre de médailles olympiques remportées représente les victoires sportives aux Jeux Olympiques. De plus, nous mesurerons la pratique sportive en France par le nombre de licenciés sportifs.

## Données
Nous exploitons des données de trois types :  
- nombre et couleur des médailles gagnées aux JO entre 2016 et 2024,
- effectifs de licenciés sportifs en France entre 2016 et 2024,
- population départementale (municipale) entre 2016 et 2023,

Ces données sont obtenues par trois sources différentes, respectivement :
- webscraping de la page Wikipédia "France aux Jeux Olympiques",
- fichiers CSV fournis par l'Injep (Institut national de la jeunesse et de l'éducation populaire), importés au format parquet car trop lourds pour un repo git, 
- requête via l'API Melodi de l'INSEE. 

La base de données de l'INJEP a l'avantage de présenter une classification fine des licenciés selon leurs caractéristiques socio-démographiques (âge, sexe, département d'exercice).

## Modèle
Nous avons utilisé trois types de modèles :
1. Modèle économétrique de panel (Approche explicative principale).
Mesurer l’effet moyen des médailles olympiques sur la croissance annuelle du nombre de licenciés par sport.
2. Modèles Ridge (Diagnostics prédictifs) : Quantifier le rôle de l’inertie temporelle par rapport aux médailles dans la prédiction du niveau de licenciés.
3. Test économétrique incrémental (Validation statistique) : Évaluer si les médailles apportent une information additionnelle dans le modèle économétrique de croissance.

## Reproduction du projet
**Installation**. Cloner le projet.
Installer les dépendances Python à l’aide de la commande :
```python
pip install -r requirements.txt
```

**Lancement**. Ouvrir le notebook `main.ipynb`, qui constitue le rapport analytique principal du projet.

## Structure du repo
- dossier `data` : dossier contenant les données brutes et receptionnant les données nettoyées, 
- dossier `jo`: package permettant la visualisation interactive des données,
- dossier `traitement`: package permettant la récupération et le nettoyage des données,
- dossier `model`: package permattant la modélisation des données,
- fichier `main.ipynb` : notebook principal servant de rapport analytique et de support de restitution.
