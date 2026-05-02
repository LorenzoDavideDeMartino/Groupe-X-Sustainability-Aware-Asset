from __future__ import annotations

from datetime import datetime

from data_cleaning_part1 import main as run_data_cleaning
from carbon_footprint_part3_1 import main as run_carbon_3_1
from carbon_comparison_part3_4 import main as run_carbon_3_4
from minimum_variance_carbon_part3_2 import main as run_min_var_carbon_3_2
from minimum_variance_part2 import main as run_min_var_2_1
from minimum_variance_part2_2 import main as run_min_var_2_2
from tracking_error_carbon_part3_3 import main as run_tracking_error_carbon_3_3
from value_weighted_part2_3 import main as run_value_weighted_2_3


def log_step(message: str) -> None:
    """Je montre clairement dans le terminal ou j'en suis."""
    print(message, flush=True)


def run_step(step_label: str, step_function) -> None:
    """
    Je lance une etape et j'affiche son temps d'execution.

    Si une etape casse, le script s'arrete tout de suite.
    """
    start_time = datetime.now()  # Je note l'heure de debut de l'etape.
    log_step(f"{step_label} - Debut")  # J'annonce le debut de l'etape.

    step_function()  # Je lance le script correspondant a l'etape.

    end_time = datetime.now()  # Je note l'heure de fin.
    duration = end_time - start_time  # Je calcule la duree totale.
    log_step(f"{step_label} - Termine en {duration}")  # J'affiche une fin claire avec la duree.


def main() -> None:
    """
    Je lance la pipeline actuelle du projet dans le bon ordre.

    J'enchaine d'abord la Part I, puis la Partie 3 bloc par bloc.
    """
    log_step("Lancement de la pipeline du projet...")  # J'annonce le lancement global.

    run_step("Etape 1/8 - Data cleaning", run_data_cleaning)  # Je commence par nettoyer les donnees.
    run_step("Etape 2/8 - Minimum Variance 2.1", run_min_var_2_1)  # Je construis ensuite l'investment set.
    run_step("Etape 3/8 - Minimum Variance 2.2", run_min_var_2_2)  # Je calcule ensuite le portefeuille minimum variance.
    run_step("Etape 4/8 - Value-Weighted 2.3", run_value_weighted_2_3)  # Je termine la Part I avec le benchmark value-weighted.
    run_step("Etape 5/8 - Carbon Footprint 3.1", run_carbon_3_1)  # Je mesure ensuite le profil carbone des deux portefeuilles de reference.
    run_step("Etape 6/8 - Minimum Variance Carbon 3.2", run_min_var_carbon_3_2)  # Je construis ensuite la version active avec contrainte carbone a 50%.
    run_step("Etape 7/8 - Tracking Error Carbon 3.3", run_tracking_error_carbon_3_3)  # Je construis ensuite la version passive avec contrainte carbone a 50%.
    run_step("Etape 8/8 - Carbon Comparison 3.4", run_carbon_3_4)  # Je termine par la comparaison des quatre portefeuilles de la Partie 3.

    log_step("Pipeline terminee.")  # Je confirme que la pipeline actuelle est finie.


if __name__ == "__main__":
    main()
