from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import pandas as pd


# Je centralise ici les chemins du projet pour que tout le script reste facile a suivre.
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "Raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"


# Je reprends ici les regles importantes du brief.
LOW_PRICE_THRESHOLD = 0.5
STALE_PRICE_THRESHOLD = 0.5
ESTIMATION_WINDOW_YEARS = 10


# Je ne garde que les fichiers vraiment utiles pour votre projet.
SCOPE1_FILE = "DS_CO2_SCOPE_1_Y_2025.xlsx"
REVENUE_FILE = "DS_REV_Y_2025.xlsx"
MARKET_VALUE_MONTHLY_FILE = "DS_MV_T_USD_M_2025.xlsx"
RETURN_INDEX_MONTHLY_FILE = "DS_RI_T_USD_M_2025.xlsx"
RISK_FREE_FILE = "Risk_Free_Rate_2025.xlsx"
STATIC_FILE = "Static_2025.xlsx"


# Je fixe des noms de sorties simples et ordonnes pour que vous puissiez les lire facilement.
OUTPUT_FILES = {
    "em_companies": "A_EM_Companies.xlsx",
    "monthly_panel": "B_EM_Monthly_Prices.xlsx",
    "annual_panel": "C_EM_Annual_Data.xlsx",
    "investment_universe": "D_EM_Investment_Universe.xlsx",
    "risk_free": "E_Risk_Free_Rate.xlsx",
    "snapshot_2024": "F_EM_2024_Snapshot.xlsx",
    "scope1_ready_2024": "G_EM_2024_Scope1_Ready.xlsx",
    "summary": "H_Cleaning_Summary.xlsx",
}


def log_step(message: str) -> None:
    """Je m'affiche dans le terminal pour que l'execution ne paraisse pas bloquee."""
    print(message, flush=True)


def extract_delisting_date(company_name: str) -> pd.Timestamp | pd.NaT:
    """Je lis la date de delisting quand Datastream l'a ajoutee dans le nom de l'entreprise."""
    if not isinstance(company_name, str):
        return pd.NaT

    match = re.search(r"DEAD - DELIST\.(\d{2}/\d{2}/\d{2})", company_name)
    if match is None:
        return pd.NaT

    return pd.to_datetime(match.group(1), format="%d/%m/%y", errors="coerce")


def load_datastream_file(file_name: str) -> pd.DataFrame:
    """
    Je charge un export Datastream brut et je retire la ligne parasite du haut.

    Pourquoi je fais cela:
    - tous les exports ont une ligne d'erreur sans ISIN,
    - je veux garder uniquement des lignes d'entreprises exploitables.
    """
    file_path = RAW_DIR / file_name
    df = pd.read_excel(file_path)
    df = df.rename(columns={"NAME": "company_name_raw", "ISIN": "isin"})
    df = df.dropna(subset=["isin"]).copy()
    df["isin"] = df["isin"].astype(str).str.strip()

    value_columns = [col for col in df.columns if col not in ["company_name_raw", "isin"]]
    for column in value_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def load_em_companies() -> pd.DataFrame:
    """
    Je construis d'abord mon univers de travail.

    Ce que je fais:
    - je pars du fichier statique,
    - je garde seulement les entreprises Emerging Markets,
    - j'ajoute la date de delisting si elle existe dans le nom.

    Pourquoi je fais cela:
    - toutes les fusions ensuite doivent se faire a partir d'un univers clair,
    - cela rend le reste du script beaucoup plus lisible.
    """
    static_df = pd.read_excel(RAW_DIR / STATIC_FILE)
    static_df = static_df.rename(
        columns={
            "ISIN": "isin",
            "NAME": "company_name",
            "Country": "country",
            "Region": "region",
        }
    )

    static_df = static_df.dropna(subset=["isin"]).drop_duplicates(subset="isin")
    static_df["isin"] = static_df["isin"].astype(str).str.strip()
    static_df["region"] = static_df["region"].astype(str).str.strip()
    static_df["delisting_date"] = static_df["company_name"].apply(extract_delisting_date)

    em_companies = static_df.loc[static_df["region"] == "EM"].copy()
    em_companies = em_companies.sort_values("isin").reset_index(drop=True)
    return em_companies


def keep_only_common_isins(em_companies: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Je garde uniquement les ISIN presents dans toutes les tables utiles.

    Pourquoi je fais cela:
    - le brief dit que si une entreprise manque completement dans une table,
      je dois la retirer de toutes les tables.
    """
    common_isins = set(em_companies["isin"])
    data_files = [
        SCOPE1_FILE,
        REVENUE_FILE,
        MARKET_VALUE_MONTHLY_FILE,
        RETURN_INDEX_MONTHLY_FILE,
    ]

    for file_name in data_files:
        file_df = load_datastream_file(file_name)
        common_isins &= set(file_df["isin"])

    filtered_companies = em_companies.loc[em_companies["isin"].isin(common_isins)].copy()
    filtered_companies = filtered_companies.sort_values("isin").reset_index(drop=True)

    stats = {
        "em_companies_before_common_filter": len(em_companies),
        "em_companies_after_common_filter": len(filtered_companies),
        "dropped_full_row_missing": len(em_companies) - len(filtered_companies),
    }
    return filtered_companies, stats


def fill_internal_missing_values(wide_df: pd.DataFrame, value_columns: list[object]) -> tuple[pd.DataFrame, int]:
    """
    Je remplis seulement les trous internes avec la valeur precedente.

    Ce que je respecte:
    - je laisse les trous au debut,
    - je laisse les trous a la fin,
    - je remplis seulement les trous entre deux valeurs connues.
    """
    cleaned_df = wide_df.copy()
    filled_cells = 0

    for row_index in cleaned_df.index:
        row_values = cleaned_df.loc[row_index, value_columns].copy()
        if row_values.notna().sum() < 2:
            continue

        first_valid = row_values.first_valid_index()
        last_valid = row_values.last_valid_index()
        internal_window = row_values.loc[first_valid:last_valid].copy()
        missing_before_fill = int(internal_window.isna().sum())

        internal_window = internal_window.ffill()
        cleaned_df.loc[row_index, internal_window.index] = internal_window.to_numpy()
        filled_cells += missing_before_fill

    return cleaned_df, filled_cells


def find_matching_month_column(
    date_columns: list[pd.Timestamp | datetime], delisting_date: pd.Timestamp
) -> pd.Timestamp | None:
    """
    Je retrouve la colonne mensuelle correspondant au mois du delisting.

    Pourquoi je fais cela:
    - Datastream utilise souvent le dernier jour de bourse du mois,
    - ce n'est pas toujours exactement la fin de mois calendaire.
    """
    matching_columns = [
        pd.Timestamp(column)
        for column in date_columns
        if pd.Timestamp(column).year == delisting_date.year
        and pd.Timestamp(column).month == delisting_date.month
    ]

    if not matching_columns:
        return None

    return max(matching_columns)


def build_monthly_prices_panel(em_companies: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Je construis le panel mensuel de prix a partir du market value et du return index.

    Ce que je fais ici:
    - je nettoie les deux exports mensuels,
    - je remplace les prix RI < 0.5 par des valeurs manquantes,
    - je mets le prix a 0 au mois du delisting,
    - je mets des valeurs manquantes apres le delisting,
    - je calcule le rendement mensuel a partir du RI.
    """
    market_value_wide = load_datastream_file(MARKET_VALUE_MONTHLY_FILE)
    return_index_wide = load_datastream_file(RETURN_INDEX_MONTHLY_FILE)

    market_value_wide = market_value_wide.loc[market_value_wide["isin"].isin(em_companies["isin"])].copy()
    return_index_wide = return_index_wide.loc[return_index_wide["isin"].isin(em_companies["isin"])].copy()

    market_value_wide = em_companies.merge(market_value_wide, on="isin", how="left")
    return_index_wide = em_companies.merge(return_index_wide, on="isin", how="left")

    market_value_columns = sorted(
        [col for col in market_value_wide.columns if isinstance(col, (datetime, pd.Timestamp))]
    )
    return_index_columns = sorted(
        [col for col in return_index_wide.columns if isinstance(col, (datetime, pd.Timestamp))]
    )

    market_value_wide, market_value_internal_fills = fill_internal_missing_values(
        market_value_wide, market_value_columns
    )
    return_index_wide, return_index_internal_fills = fill_internal_missing_values(
        return_index_wide, return_index_columns
    )

    low_price_mask = return_index_wide[return_index_columns].lt(LOW_PRICE_THRESHOLD)
    low_price_mask = low_price_mask & return_index_wide[return_index_columns].notna()
    low_price_count = int(low_price_mask.sum().sum())
    return_index_wide.loc[:, return_index_columns] = return_index_wide[return_index_columns].mask(low_price_mask)

    delisting_count = 0
    forced_zero_count = 0
    missing_after_delisting_count_mv = 0
    missing_after_delisting_count_ri = 0

    for row_index in em_companies.index:
        delisting_date = em_companies.at[row_index, "delisting_date"]
        if pd.isna(delisting_date):
            continue

        market_value_month = find_matching_month_column(market_value_columns, pd.Timestamp(delisting_date))
        return_index_month = find_matching_month_column(return_index_columns, pd.Timestamp(delisting_date))

        if market_value_month is None or return_index_month is None:
            continue

        delisting_count += 1

        later_market_value_columns = [col for col in market_value_columns if pd.Timestamp(col) > market_value_month]
        later_return_index_columns = [col for col in return_index_columns if pd.Timestamp(col) > return_index_month]

        missing_after_delisting_count_mv += int(
            market_value_wide.loc[row_index, later_market_value_columns].notna().sum()
        )
        missing_after_delisting_count_ri += int(
            return_index_wide.loc[row_index, later_return_index_columns].notna().sum()
        )

        market_value_wide.loc[row_index, later_market_value_columns] = pd.NA
        return_index_wide.loc[row_index, later_return_index_columns] = pd.NA

        market_value_wide.at[row_index, market_value_month] = 0.0
        return_index_wide.at[row_index, return_index_month] = 0.0
        forced_zero_count += 1

    id_columns = ["isin", "company_name", "country", "region", "delisting_date"]

    market_value_long = market_value_wide.melt(
        id_vars=id_columns,
        value_vars=market_value_columns,
        var_name="date",
        value_name="market_value_usd",
    )
    return_index_long = return_index_wide.melt(
        id_vars=id_columns,
        value_vars=return_index_columns,
        var_name="date",
        value_name="return_index",
    )

    monthly_panel = market_value_long.merge(
        return_index_long[["isin", "date", "return_index"]],
        on=["isin", "date"],
        how="outer",
    )

    monthly_panel["date"] = pd.to_datetime(monthly_panel["date"])
    monthly_panel = monthly_panel.sort_values(["isin", "date"]).reset_index(drop=True)

    monthly_panel["return_index_lag"] = monthly_panel.groupby("isin")["return_index"].shift(1)
    monthly_panel["monthly_return"] = (
        monthly_panel["return_index"] / monthly_panel["return_index_lag"] - 1
    )

    invalid_return_mask = monthly_panel["return_index"].isna() | monthly_panel["return_index_lag"].isna()
    monthly_panel.loc[invalid_return_mask, "monthly_return"] = pd.NA

    monthly_panel["is_delisting_month"] = (
        monthly_panel["delisting_date"].notna()
        & (monthly_panel["date"].dt.to_period("M") == monthly_panel["delisting_date"].dt.to_period("M"))
        & monthly_panel["return_index"].eq(0)
    )

    monthly_panel = monthly_panel[
        [
            "isin",
            "company_name",
            "country",
            "region",
            "delisting_date",
            "date",
            "market_value_usd",
            "return_index",
            "monthly_return",
            "is_delisting_month",
        ]
    ]

    stats = {
        "market_value_internal_gaps_filled": market_value_internal_fills,
        "return_index_internal_gaps_filled": return_index_internal_fills,
        "ri_prices_below_0_5_set_to_missing": low_price_count,
        "delisting_events_applied": delisting_count,
        "months_forced_to_zero": forced_zero_count,
        "market_value_cells_removed_after_delisting": missing_after_delisting_count_mv,
        "return_index_cells_removed_after_delisting": missing_after_delisting_count_ri,
    }
    return monthly_panel, stats


def build_annual_data_panel(em_companies: pd.DataFrame, monthly_panel: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Je construis le panel annuel utile pour le projet.

    Ce que je garde:
    - Scope 1,
    - Revenue,
    - prix de fin d'annee issus du panel mensuel nettoye.

    Pourquoi je procede ainsi:
    - je n'ai plus besoin de Scope 2,
    - je veux que les prix de fin d'annee soient coherents avec les regles sur
      les delistings et les low prices.
    """
    scope1_wide = load_datastream_file(SCOPE1_FILE)
    revenue_wide = load_datastream_file(REVENUE_FILE)

    scope1_wide = scope1_wide.loc[scope1_wide["isin"].isin(em_companies["isin"])].copy()
    revenue_wide = revenue_wide.loc[revenue_wide["isin"].isin(em_companies["isin"])].copy()

    scope1_year_columns = sorted([col for col in scope1_wide.columns if isinstance(col, int)])
    revenue_year_columns = sorted([col for col in revenue_wide.columns if isinstance(col, int)])

    scope1_wide, scope1_internal_fills = fill_internal_missing_values(scope1_wide, scope1_year_columns)
    revenue_wide, revenue_internal_fills = fill_internal_missing_values(revenue_wide, revenue_year_columns)

    scope1_long = scope1_wide.melt(
        id_vars=["isin"],
        value_vars=scope1_year_columns,
        var_name="year",
        value_name="scope1_co2",
    )
    revenue_long = revenue_wide.melt(
        id_vars=["isin"],
        value_vars=revenue_year_columns,
        var_name="year",
        value_name="revenue_usd",
    )

    scope1_long["year"] = scope1_long["year"].astype(int)
    revenue_long["year"] = revenue_long["year"].astype(int)

    annual_panel = em_companies.merge(scope1_long, on="isin", how="left")
    annual_panel = annual_panel.merge(revenue_long, on=["isin", "year"], how="left")

    year_end_prices = monthly_panel.loc[monthly_panel["date"].dt.month == 12].copy()
    year_end_prices["year"] = year_end_prices["date"].dt.year
    year_end_prices = year_end_prices.rename(
        columns={
            "market_value_usd": "year_end_market_value_usd",
            "return_index": "year_end_return_index",
        }
    )
    year_end_prices["price_available_eoy"] = year_end_prices["year_end_return_index"].notna()

    annual_panel = annual_panel.merge(
        year_end_prices[
            [
                "isin",
                "year",
                "year_end_market_value_usd",
                "year_end_return_index",
                "price_available_eoy",
            ]
        ],
        on=["isin", "year"],
        how="left",
    )

    annual_panel = annual_panel.sort_values(["isin", "year"]).reset_index(drop=True)

    stats = {
        "scope1_internal_gaps_filled": scope1_internal_fills,
        "revenue_internal_gaps_filled": revenue_internal_fills,
    }
    return annual_panel, stats


def build_investment_universe(monthly_panel: pd.DataFrame) -> pd.DataFrame:
    """
    Je construis ici l'univers investissable annee par annee.

    Ce que je fais:
    - si le prix de fin d'annee Y manque, je n'investis pas en Y+1,
    - je regarde sur les 10 annees precedentes la part des rendements egaux a 0,
    - si cette part depasse 50%, je considere l'action comme stale.
    """
    year_end_rows = monthly_panel.loc[monthly_panel["date"].dt.month == 12].copy()
    year_end_rows["formation_year"] = year_end_rows["date"].dt.year
    year_end_rows["investment_year"] = year_end_rows["formation_year"] + 1
    year_end_rows["price_available_eoy"] = year_end_rows["return_index"].notna()

    results = []

    for formation_year in sorted(year_end_rows["formation_year"].unique()):
        window_start = pd.Timestamp(formation_year - ESTIMATION_WINDOW_YEARS + 1, 1, 1)
        window_end = pd.Timestamp(formation_year, 12, 31)

        window_data = monthly_panel.loc[
            (monthly_panel["date"] >= window_start) & (monthly_panel["date"] <= window_end),
            ["isin", "monthly_return"],
        ].copy()

        stale_stats = (
            window_data.groupby("isin")["monthly_return"]
            .agg(
                valid_return_count_10y=lambda values: int(values.notna().sum()),
                zero_return_count_10y=lambda values: int((values.eq(0) & values.notna()).sum()),
            )
            .reset_index()
        )

        stale_stats["zero_return_ratio_10y"] = (
            stale_stats["zero_return_count_10y"] / stale_stats["valid_return_count_10y"]
        )
        stale_stats.loc[
            stale_stats["valid_return_count_10y"] == 0, "zero_return_ratio_10y"
        ] = pd.NA
        stale_stats["stale_price_flag"] = stale_stats["zero_return_ratio_10y"] > STALE_PRICE_THRESHOLD
        stale_stats["stale_price_flag"] = stale_stats["stale_price_flag"].fillna(False)

        year_slice = year_end_rows.loc[year_end_rows["formation_year"] == formation_year].copy()
        year_slice = year_slice.merge(stale_stats, on="isin", how="left")
        year_slice["valid_return_count_10y"] = year_slice["valid_return_count_10y"].fillna(0).astype(int)
        year_slice["zero_return_count_10y"] = year_slice["zero_return_count_10y"].fillna(0).astype(int)
        year_slice["stale_price_flag"] = year_slice["stale_price_flag"].fillna(False)
        year_slice["investable_next_year"] = (
            year_slice["price_available_eoy"] & (~year_slice["stale_price_flag"])
        )

        results.append(
            year_slice[
                [
                    "isin",
                    "company_name",
                    "country",
                    "region",
                    "delisting_date",
                    "formation_year",
                    "investment_year",
                    "market_value_usd",
                    "return_index",
                    "price_available_eoy",
                    "valid_return_count_10y",
                    "zero_return_count_10y",
                    "zero_return_ratio_10y",
                    "stale_price_flag",
                    "investable_next_year",
                ]
            ].rename(
                columns={
                    "market_value_usd": "year_end_market_value_usd",
                    "return_index": "year_end_return_index",
                }
            )
        )

    investment_universe = pd.concat(results, ignore_index=True)
    investment_universe = investment_universe.sort_values(["formation_year", "isin"]).reset_index(drop=True)
    return investment_universe


def build_snapshot_files(annual_panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Je prepare enfin deux vues simples pour la suite du projet.

    Pourquoi 2024:
    - les emissions 2025 sont encore trop peu completes,
    - 2024 est plus exploitable pour Scope 1.
    """
    snapshot_2024 = annual_panel.loc[annual_panel["year"] == 2024].copy()
    scope1_ready_2024 = snapshot_2024.dropna(
        subset=[
            "scope1_co2",
            "revenue_usd",
            "year_end_market_value_usd",
            "year_end_return_index",
        ]
    ).copy()

    snapshot_2024 = snapshot_2024.sort_values("isin").reset_index(drop=True)
    scope1_ready_2024 = scope1_ready_2024.sort_values("isin").reset_index(drop=True)
    return snapshot_2024, scope1_ready_2024


def build_summary_table(*stats_dicts: dict[str, int]) -> pd.DataFrame:
    """Je rassemble toutes les statistiques de nettoyage dans un tableau final."""
    rows: list[dict[str, object]] = []

    for stats in stats_dicts:
        for metric, value in stats.items():
            rows.append({"metric": metric, "value": value})

    return pd.DataFrame(rows)


def write_excel_with_fallback(df: pd.DataFrame, file_name: str) -> Path:
    """
    J'ecris un fichier Excel, et si le fichier est ouvert je cree une version _new.

    Pourquoi je fais cela:
    - Excel verrouille souvent les fichiers ouverts,
    - je prefere sauver le resultat plutot que faire echouer tout le script.
    """
    target_path = PROCESSED_DIR / file_name

    try:
        df.to_excel(target_path, index=False)
        return target_path
    except PermissionError:
        fallback_path = target_path.with_name(f"{target_path.stem}_new{target_path.suffix}")
        df.to_excel(fallback_path, index=False)
        return fallback_path


def save_outputs(
    em_companies: pd.DataFrame,
    monthly_panel: pd.DataFrame,
    annual_panel: pd.DataFrame,
    investment_universe: pd.DataFrame,
    risk_free_rate: pd.DataFrame,
    snapshot_2024: pd.DataFrame,
    scope1_ready_2024: pd.DataFrame,
    summary_table: pd.DataFrame,
) -> dict[str, str]:
    """
    Je sauvegarde maintenant les sorties finales dans un ordre logique.

    Ordre choisi:
    - A: entreprises EM,
    - B: prix mensuels nettoyes,
    - C: donnees annuelles utiles,
    - D: univers investissable,
    - E: taux sans risque,
    - F/G: vues 2024,
    - H: resume du nettoyage.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    written_files = {
        "A": str(write_excel_with_fallback(em_companies, OUTPUT_FILES["em_companies"])),
        "B": str(write_excel_with_fallback(monthly_panel, OUTPUT_FILES["monthly_panel"])),
        "C": str(write_excel_with_fallback(annual_panel, OUTPUT_FILES["annual_panel"])),
        "D": str(write_excel_with_fallback(investment_universe, OUTPUT_FILES["investment_universe"])),
        "E": str(write_excel_with_fallback(risk_free_rate, OUTPUT_FILES["risk_free"])),
        "F": str(write_excel_with_fallback(snapshot_2024, OUTPUT_FILES["snapshot_2024"])),
        "G": str(write_excel_with_fallback(scope1_ready_2024, OUTPUT_FILES["scope1_ready_2024"])),
        "H": str(write_excel_with_fallback(summary_table, OUTPUT_FILES["summary"])),
    }
    return written_files


def clean_risk_free_rate() -> pd.DataFrame:
    """
    Je nettoie le taux sans risque mensuel.

    Pourquoi je fais cela:
    - je veux l'avoir sous une forme simple et directement reutilisable plus tard.
    """
    risk_free_df = pd.read_excel(RAW_DIR / RISK_FREE_FILE)
    risk_free_df = risk_free_df.rename(columns={"Unnamed: 0": "yyyymm", "RF": "rf_percent"})
    risk_free_df["yyyymm"] = risk_free_df["yyyymm"].astype(str).str.strip()
    risk_free_df["date"] = pd.to_datetime(risk_free_df["yyyymm"] + "01", format="%Y%m%d")
    risk_free_df["date"] = risk_free_df["date"] + pd.offsets.MonthEnd(0)
    risk_free_df["rf_percent"] = pd.to_numeric(risk_free_df["rf_percent"], errors="coerce")
    risk_free_df["rf_decimal"] = risk_free_df["rf_percent"] / 100

    risk_free_df = risk_free_df[["date", "rf_percent", "rf_decimal"]]
    risk_free_df = risk_free_df.sort_values("date").reset_index(drop=True)
    return risk_free_df


def main() -> None:
    # Je commence par definir l'univers de base Emerging Markets.
    log_step("Etape 1/7 - Je charge les entreprises Emerging Markets...")
    em_companies = load_em_companies()

    # Je retire ensuite les entreprises qui manquent completement dans une table utile.
    log_step("Etape 2/7 - Je garde seulement les ISIN presents dans toutes les tables utiles...")
    em_companies, common_isin_stats = keep_only_common_isins(em_companies)

    # Je nettoie ensuite les prix mensuels, car ils servent aussi a construire les regles d'investissement.
    log_step("Etape 3/7 - Je nettoie les prix mensuels et je calcule les rendements...")
    monthly_panel, monthly_stats = build_monthly_prices_panel(em_companies)

    # Je nettoie ensuite les donnees annuelles vraiment utiles pour votre projet.
    log_step("Etape 4/7 - Je construis les donnees annuelles utiles (Scope 1, revenue, prix fin d'annee)...")
    annual_panel, annual_stats = build_annual_data_panel(em_companies, monthly_panel)

    # Je construis maintenant l'univers investissable annee par annee.
    log_step("Etape 5/7 - Je construis l'univers investissable avec le filtre stale prices...")
    investment_universe = build_investment_universe(monthly_panel)

    # Je nettoie aussi le taux sans risque pour l'avoir deja pret pour la suite.
    log_step("Etape 6/7 - Je nettoie le taux sans risque et je prepare les fichiers 2024...")
    risk_free_rate = clean_risk_free_rate()
    snapshot_2024, scope1_ready_2024 = build_snapshot_files(annual_panel)

    # Je termine par les exports Excel ordonnes de A a H.
    log_step("Etape 7/7 - J'enregistre les fichiers Excel finaux...")
    summary_table = build_summary_table(common_isin_stats, monthly_stats, annual_stats)
    written_files = save_outputs(
        em_companies=em_companies,
        monthly_panel=monthly_panel,
        annual_panel=annual_panel,
        investment_universe=investment_universe,
        risk_free_rate=risk_free_rate,
        snapshot_2024=snapshot_2024,
        scope1_ready_2024=scope1_ready_2024,
        summary_table=summary_table,
    )

    log_step("Nettoyage termine.")
    log_step(f"Nombre d'entreprises EM retenues: {len(em_companies)}")
    log_step(f"Lignes du panel mensuel: {len(monthly_panel)}")
    log_step(f"Lignes du panel annuel: {len(annual_panel)}")
    log_step(f"Lignes de l'univers investissable: {len(investment_universe)}")
    log_step("Fichiers ecrits:")
    for label, path in written_files.items():
        log_step(f"{label} -> {path}")


if __name__ == "__main__":
    main()
