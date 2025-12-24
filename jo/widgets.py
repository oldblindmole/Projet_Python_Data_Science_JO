"""
Widgets ipywidgets.
_un seul Output_, géré par _run_widget.
"""

import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output

from .visualisation_plotly import (
    graphique_licences_et_medailles,
    licences_par_annee,
    evolution_licencies_age,
    evolution_licences_tranches_fines_age,
    evolution_licences_tranches_grandes_age,
    repartition_grandes_tranches_age_par_sport,
    repartition_fines_tranches_age_par_sport,
    plot_licences_par_sexe,
    plot_part_femmes,
    plot_part_jeunes,
    plot_heatmap_nbr_licencies,
)
from .visualisation_cartes import (
    carte_licencies,
    carte_evolution_licencies,
)


def _run_widget(update_fn, controls):
    """
    Affiche des contrôles + un Output, et relance update_fn() quand un contrôle change.

    Paramètres
    ----------
    update_fn : callable
        Fonction sans argument qui trace/affiche (elle lit les valeurs via closure).
    controls : list
        Liste de widgets (Dropdown, Slider...) à observer.

    Retour
    ------
    out : widgets.Output
        Output widget dans lequel sont affichés les graphiques.
    """
    out = widgets.Output()

    def _wrapped_update(change=None):  # pylint: disable=W0613
        with out:
            clear_output(wait=True)
            res = update_fn()
            if res is not None:
                display(res)

    for c in controls:
        c.observe(_wrapped_update, names="value")

    display(*controls, out)
    _wrapped_update()


def widget_graphique_licences_et_medailles(df):
    """TODO"""
    sports = ["all"] + sorted(df["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    def update():
        return graphique_licences_et_medailles(df, sport=sport_widget.value)

    _run_widget(update, [sport_widget])


def widget_licences_par_sport(df):
    """
    Crée un widget interactif (Dropdown) permettant de sélectionner un sport
    et d'afficher le graphique correspondant via la fonction `plot_licences_par_annee`.

    Paramètres
    ----------
    df : pandas.DataFrame
        DataFrame contenant au minimum les colonnes :
        - sport_name_col (par défaut 'sport')
        - sport_code_col (par défaut 'code_sport')
    sport_name_col : str, optionnel
        Nom de la colonne contenant le libellé du sport (par défaut 'sport').
    sport_code_col : str, optionnel
        Nom de la colonne contenant le code du sport (par défaut 'code_sport').

    Retour
    ------
    None
        Affiche le widget et le graphique dans le notebook.

    """
    sports = ["all"] + sorted(df["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    def update():
        return licences_par_annee(df, sport_code=sport_widget.value)

    _run_widget(update, [sport_widget])


def widgets_carte_licencies(data_complet, gdf_dep, data_pop):
    """
    Construit et affiche un widget interactif pour la carte de proportion de licenciés.

    Le widget permet de sélectionner :
    - une année
    - un sport (ou "all")

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète.
    gdf_dep : geopandas.GeoDataFrame
        Géométrie des départements.
    data_pop : pandas.DataFrame
        Population de référence.

    Retour
    ------
    None
        Affiche les widgets et la figure dans le notebook.
    """
    # Années et sports disponibles
    annees = sorted(data_complet["annee"].dropna().unique())
    sports = ["all"] + sorted(data_complet["sport"].dropna().unique())

    # Création des widgets Dropdown
    annee_widget = widgets.Dropdown(
        options=annees, description="Année :", value=annees[0]
    )
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    # Fonction de mise à jour
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour la carte interactive lorsque l'utilisateur change l'année ou le sport.
        """
        carte_licencies(
            data_complet,
            gdf_dep,
            data_pop,
            annee=annee_widget.value,
            sport=sport_widget.value,
        )

    _run_widget(update, [annee_widget, sport_widget])


def widgets_evolution_licencies(data_complet, gdf_dep):
    """
    Construit et affiche un widget interactif pour la carte d'évolution des licenciés.

    Le widget permet de sélectionner :
    - une année 1 (référence)
    - une année 2 (comparaison)
    - un sport (ou "all")

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète.
    gdf_dep : geopandas.GeoDataFrame
        Géométrie des départements.

    Retour
    ------
    None
        Affiche les widgets et la figure dans le notebook.
    """
    # Années et sports disponibles
    annees = sorted(data_complet["annee"].dropna().unique())
    sports = ["all"] + sorted(data_complet["sport"].dropna().unique())

    # Création des widgets Dropdown
    annee1_widget = widgets.Dropdown(
        options=annees, description="Année 1 :", value=annees[0]
    )
    annee2_widget = widgets.Dropdown(
        options=annees,
        description="Année 2 :",
        value=annees[1] if len(annees) > 1 else annees[0],
    )
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    # Fonction de mise à jour
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour la carte interactive lorsque l'utilisateur change l'année ou le sport.
        """
        carte_evolution_licencies(
            data_complet,
            gdf_dep,
            annee1=annee1_widget.value,
            annee2=annee2_widget.value,
            sport=sport_widget.value,
        )

    _run_widget(update, [annee1_widget, annee2_widget, sport_widget])


def widgets_evolution_licencies_age(data_complet):
    """
    Widget interactif pour afficher l'évolution des licenciés par âge exact.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant la colonne `age`.

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Tri des les âges numériques, on conserve "NR - Non réparti" à part
    ages_numeric = sorted(
        [a for a in data_complet["age"].dropna().unique() if a != "NR - Non réparti"],
        key=int,
    )
    # Ajout de "NR - Non réparti" à la fin si présent
    ages = ages_numeric + (
        ["NR - Non réparti"]
        if "NR - Non réparti" in data_complet["age"].unique()
        else []
    )

    # Création du widget Dropdown
    age_widget = widgets.Dropdown(
        options=["all"] + ages, description="Age :", value="all"
    )

    # Fonction de mise à jour du graphique
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour le graphique interactif lorsque l'utilisateur change l'âge sélectionné.
        """
        return evolution_licencies_age(data_complet, age=age_widget.value)

    _run_widget(update, [age_widget])


def widgets_evolution_licences_tranches_fines_age(data_complet):
    """
    Widget interactif pour afficher l'évolution des licenciés par tranche d'âge fine.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant la colonne `tranche_age`.

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Récupération et tri des tranches d'âge uniques
    tranches = sorted(data_complet["tranche_age"].dropna().unique())

    # Création du widget Dropdown
    tranche_widget = widgets.Dropdown(
        options=["all"] + tranches, description="Tranche :", value="all"
    )

    # Fonction de mise à jour du graphique
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour le graphique interactif lorsque l'utilisateur change la tranche sélectionnée.
        """
        return evolution_licences_tranches_fines_age(
            data_complet, tranche=tranche_widget.value
        )

    _run_widget(update, [tranche_widget])


def widgets_evolution_licences_tranches_grande_age(data_complet):
    """
    Widget interactif pour afficher l'évolution des licenciés par grande tranche d'âge.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant la colonne `grande_tranche_age`.

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Récupération et tri des tranches d'âge uniques
    tranches = sorted(data_complet["grande_tranche_age"].dropna().unique())

    # Création du widget Dropdown
    tranche_widget = widgets.Dropdown(
        options=["all"] + tranches, description="Tranche :", value="all"
    )

    # Fonction de mise à jour du graphique
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour le graphique interactif lorsque l'utilisateur change la tranche sélectionnée.
        """
        return evolution_licences_tranches_grandes_age(
            data_complet, tranche=tranche_widget.value
        )

    _run_widget(update, [tranche_widget])


def widgets_repartition_grandes_tranches_age_par_sport(data_complet):
    """
    Widget interactif pour la répartition des licenciés par grande tranche d'âge.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant `annee` et `grande_tranche_age`.

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Récupération des années uniques et triées
    annees = sorted(data_complet["annee"].dropna().unique())

    # Création du widget Dropdown
    annees_widget = widgets.Dropdown(
        options=["all"] + list(annees), description="Année :", value="all"
    )

    # Fonction de mise à jour du graphique
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour le graphique interactif lorsque l'utilisateur change l'année sélectionnée.
        """
        return repartition_grandes_tranches_age_par_sport(
            data_complet, annee=annees_widget.value
        )

    _run_widget(update, [annees_widget])


def widgets_repartition_fines_tranches_age_par_sport(data_complet):
    """
    Widget interactif pour la répartition des licenciés par tranche d'âge fine.

    Paramètres
    ----------
    data_complet : pandas.DataFrame
        Base complète contenant `annee` et `tranche_age`.

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Récupération des années uniques et triées
    annees = sorted(data_complet["annee"].dropna().unique())

    # Création du widget Dropdown
    annees_widget = widgets.Dropdown(
        options=["all"] + list(annees), description="Année :", value="all"
    )

    # Fonction de mise à jour du graphique
    def update(change=None):  # pylint: disable=W0613
        """
        Met à jour le graphique interactif lorsque l'utilisateur change l'année sélectionnée.
        """
        return repartition_fines_tranches_age_par_sport(
            data_complet, annee=annees_widget.value
        )

    _run_widget(update, [annees_widget])


def widgets_licences_par_sexe(df_lic, sport_col="code_sport"):
    """
    Crée un widget de sélection du sport pour afficher les licences par sexe.

    Paramètres
    ----------
    df_lic : pandas.DataFrame
        Base des licenciés.
    sport_col : str, optionnel
        Nom de la colonne contenant le code du sport utilisé pour le filtrage
        (par défaut "code_sport").

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """

    # Liste des sports disponibles pour le widget
    sports = ["all"] + sorted(df_lic["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    def update():
        """Fonction de mise à jour appelée lors d'un changement de sélection."""
        selected = sport_widget.value

        # Cas : tous les sports
        if selected == "all":
            return plot_licences_par_sexe(df_lic, sport_code="all", sport_col=sport_col)

        # Récupération du ou des codes associés au sport sélectionné
        codes = df_lic.loc[df_lic["sport"] == selected, sport_col].dropna().unique()

        # Si plusieurs codes existent, on les passe sous forme de liste
        code = codes[0] if len(codes) == 1 else list(codes)

        return plot_licences_par_sexe(df_lic, sport_code=code, sport_col=sport_col)

    _run_widget(update, [sport_widget])


def widgets_part_jeunes(df_lic: pd.DataFrame, sport_col: str = "code_sport"):
    """
    Crée un widget interactif pour visualiser la part de jeunes licenciés.

    Paramètres
    ----------
    df_lic : pandas.DataFrame
        Base des licenciés contenant au minimum les colonnes :
        - 'sport' : nom du sport (str)
        - sport_col : code du sport (par défaut 'code_sport')
        - toutes les colonnes nécessaires à `plot_part_jeunes`
    sport_col : str, optionnel
        Nom de la colonne contenant le code sport (par défaut "code_sport").

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """

    # Liste des sports disponibles
    sports = ["all"] + sorted(df_lic["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    # Slider de choix du seuil d'âge (âge max)
    age_widget = widgets.IntSlider(
        value=15, min=5, max=30, step=1, description="Âge max :"
    )

    def update():
        """Met à jour la figure en fonction du sport et du seuil d'âge sélectionnés."""
        selected = sport_widget.value
        age_max = age_widget.value

        # Cas : tous les sports
        if selected == "all":
            return plot_part_jeunes(
                df_lic, age_max=age_max, sport_code="all", sport_col=sport_col
            )

        # Récupération du ou des codes associés au sport sélectionné
        codes = df_lic.loc[df_lic["sport"] == selected, sport_col].dropna().unique()

        # Si plusieurs codes existent, on les passe sous forme de liste
        code = codes[0] if len(codes) == 1 else list(codes)

        return plot_part_jeunes(
            df_lic, age_max=age_max, sport_code=code, sport_col=sport_col
        )

    _run_widget(update, [sport_widget, age_widget])


def widgets_part_femmes(df_lic, sport_col="code_sport"):
    """
    Crée un widget interactif pour visualiser la part de femmes parmi les licenciés.

    Paramètres
    ----------
    df_lic : pandas.DataFrame
        Base des licenciés contenant au minimum les colonnes :
        - 'sport' : nom du sport (str)
        - sport_col : code du sport (par défaut 'code_sport')
        - toutes les colonnes nécessaires à `plot_part_femmes`
    sport_col : str, optionnel
        Nom de la colonne contenant le code sport (par défaut "code_sport").

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """

    # Liste des sports disponibles
    sports = ["all"] + sorted(df_lic["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    def update():
        """Met à jour la figure en fonction du sport et du seuil d'âge sélectionnés."""
        selected = sport_widget.value

        # Cas : tous les sports
        if selected == "all":
            return plot_part_femmes(df_lic, sport_code="all", sport_col=sport_col)

        # Récupération du ou des codes associés au sport sélectionné
        codes = df_lic.loc[df_lic["sport"] == selected, sport_col].dropna().unique()

        # Si plusieurs codes existent, on les passe sous forme de liste
        code = codes[0] if len(codes) == 1 else list(codes)

        return plot_part_femmes(df_lic, sport_code=code, sport_col=sport_col)

    _run_widget(update, [sport_widget])


def widgets_heatmap_nbr_licencies(df_lic: pd.DataFrame, sport_col: str = "code_sport"):
    """
    Crée un widget interactif pour visualiser une heatmap du nombre de licences.

    Paramètres
    ----------
    df_lic : pandas.DataFrame
        Base des licenciés contenant au minimum les colonnes :
        - 'sport' : nom du sport (str)
        - sport_col : code du sport (par défaut 'code_sport')
        - toutes les colonnes nécessaires à `plot_heatmap_nbr_licencies`
    sport_col : str, optionnel
        Nom de la colonne contenant le code sport (par défaut "code_sport").

    Retour
    ------
    None
        Affiche un widget + le graphique Plotly associé.
    """
    # Liste des sports disponibles
    sports = ["all"] + sorted(df_lic["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    def update():
        """Met à jour la heatmap en fonction du sport sélectionné."""
        selected = sport_widget.value

        # Cas : tous les sports
        if selected == "all":
            return plot_heatmap_nbr_licencies(
                df_lic, sport_code="all", sport_col=sport_col
            )

        # Récupération du ou des codes associés au sport sélectionné
        codes = df_lic.loc[df_lic["sport"] == selected, sport_col].dropna().unique()

        # Si plusieurs codes existent, on les passe sous forme de liste
        code = codes[0] if len(codes) == 1 else list(codes)

        return plot_heatmap_nbr_licencies(df_lic, sport_code=code, sport_col=sport_col)

    _run_widget(update, [sport_widget])
