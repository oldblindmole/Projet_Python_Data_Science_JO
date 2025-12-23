"""
Widgets ipywidgets.
_un seul Output_, géré par _run_widget.
"""

import ipywidgets as widgets
from IPython.display import display, clear_output

from visualisation_plotly import (
    graphique_licences_et_medailles,
    licences_par_annee,
    plot_part_femmes,
    plot_part_jeunes,
    plot_heatmap_nbr_licencies,
)
from visualisation_cartes import (
    carte_licencies,
    carte_evolution_licencies
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

    def _wrapped_update(change=None):
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
    """TODO"""
    sports = ["all"] + sorted(df["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    def update():
        return licences_par_annee(df, sport_code=sport_widget.value)

    _run_widget(update, [sport_widget])


def widgets_part_femmes(df):
    """TODO"""
    sports = ["all"] + sorted(df["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    def update():
        return plot_part_femmes(df, sport=sport_widget.value)

    _run_widget(update, [sport_widget])


def widgets_part_jeunes(df):
    """TODO"""
    sports = ["all"] + sorted(df["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")
    age_widget = widgets.IntSlider(value=15, min=5, max=30, step=1, description="Âge max")

    def update():
        return plot_part_jeunes(df, age_max=age_widget.value, sport=sport_widget.value)

    _run_widget(update, [sport_widget, age_widget])


def widgets_heatmap_nbr_licencies(df):
    """TODO"""
    sports = ["all"] + sorted(df["sport"].dropna().unique())
    sport_widget = widgets.Dropdown(options=sports, description="Sport :", value="all")

    def update():
        return plot_heatmap_nbr_licencies(df, sport=sport_widget.value)

    _run_widget(update, [sport_widget])
