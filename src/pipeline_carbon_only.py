from __future__ import annotations

from datetime import datetime

from carbon_footprint_part3_1 import main as run_carbon_3_1
from carbon_comparison_part3_4 import main as run_carbon_3_4
from minimum_variance_carbon_part3_2 import main as run_min_var_carbon_3_2
from net_zero_part4_1 import main as run_net_zero_4_1
from passive_comparison_part4_2 import main as run_passive_comparison_4_2
from tracking_error_carbon_part3_3 import main as run_tracking_error_carbon_3_3


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
    Je lance uniquement la suite carbone du projet.

    Je pars des fichiers deja presents dans data/processed.
    Je ne relance ni le data cleaning, ni la Part I.
    """
    log_step("Lancement de la pipeline carbone uniquement...")  # J'annonce le lancement global.

    run_step("Etape 1/6 - Carbon Footprint 3.1", run_carbon_3_1)  # Je reconstruis d'abord les metriques carbone de reference.
    run_step("Etape 2/6 - Minimum Variance Carbon 3.2", run_min_var_carbon_3_2)  # Je construis ensuite la version active avec contrainte carbone a 50%.
    run_step("Etape 3/6 - Tracking Error Carbon 3.3", run_tracking_error_carbon_3_3)  # Je construis ensuite la version passive avec contrainte carbone a 50%.
    run_step("Etape 4/6 - Carbon Comparison 3.4", run_carbon_3_4)  # Je compare ensuite les quatre portefeuilles de la Partie 3.
    run_step("Etape 5/6 - Net Zero 4.1", run_net_zero_4_1)  # Je construis ensuite la strategie net-zero passive.
    run_step("Etape 6/6 - Passive Comparison 4.2", run_passive_comparison_4_2)  # Je termine par la comparaison finale des strategies passives.

    log_step("Pipeline carbone terminee.")  # Je confirme que la suite carbone est finie.


if __name__ == "__main__":
    main()
