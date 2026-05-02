from __future__ import annotations

from datetime import datetime

from data_cleaning_part1 import main as run_data_cleaning
from minimum_variance_part2 import main as run_min_var_2_1
from minimum_variance_part2_2 import main as run_min_var_2_2
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

    Pour l'instant, cette pipeline couvre la Part I.
    Plus tard, on pourra ajouter les parties suivantes dans ce meme fichier.
    """
    log_step("Lancement de la pipeline du projet...")  # J'annonce le lancement global.

    run_step("Etape 1/4 - Data cleaning", run_data_cleaning)  # Je commence par nettoyer les donnees.
    run_step("Etape 2/4 - Minimum Variance 2.1", run_min_var_2_1)  # Je construis ensuite l'investment set.
    run_step("Etape 3/4 - Minimum Variance 2.2", run_min_var_2_2)  # Je calcule ensuite le portefeuille minimum variance.
    run_step("Etape 4/4 - Value-Weighted 2.3", run_value_weighted_2_3)  # Je termine par le benchmark value-weighted.

    log_step("Pipeline terminee.")  # Je confirme que la pipeline actuelle est finie.


if __name__ == "__main__":
    main()
