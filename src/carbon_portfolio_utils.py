from __future__ import annotations

from pathlib import Path
import warnings

import numpy as np
import pandas as pd
from scipy.optimize import minimize


# Je centralise ici les chemins du projet.
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
RAW_DIR = BASE_DIR / "data" / "Raw"


# Je borne toutes les sections carbone exactement comme dans le projet.
FIRST_FORMATION_YEAR = 2013
LAST_FORMATION_YEAR = 2024
INITIAL_WEALTH_USD = 1_000_000.0


def log_step(message: str) -> None:
    """Je montre clairement l'avancement dans le terminal."""
    print(message, flush=True)


def write_excel(df: pd.DataFrame, file_name: str) -> Path:
    """J'ecris un fichier Excel simple dans data/processed."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    target_path = PROCESSED_DIR / file_name
    df.to_excel(target_path, index=False)
    return target_path


def write_workbook(file_name: str, sheets: dict[str, pd.DataFrame]) -> Path:
    """J'ecris plusieurs feuilles dans un seul fichier Excel."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    target_path = PROCESSED_DIR / file_name
    with pd.ExcelWriter(target_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            safe_name = sheet_name[:31]
            df.to_excel(writer, sheet_name=safe_name, index=False)
    return target_path


def standardize_isin(series: pd.Series) -> pd.Series:
    """Je nettoie les ISIN tout de suite apres chargement."""
    return series.astype(str).str.strip()


def compute_summary_stats(returns_series, rf_series) -> dict[str, float]:
    """
    Je calcule les statistiques demandees dans le projet.

    Annualisation:
    - mean * 12
    - std * sqrt(12)
    - Sharpe = mean excess annualise / volatilite annualisee
    """
    returns = pd.Series(returns_series).astype(float).dropna()

    if isinstance(rf_series, pd.Series):
        rf_aligned = rf_series.reindex(returns.index)
    else:
        rf_aligned = pd.Series(rf_series, index=returns.index)

    excess_returns = returns - rf_aligned
    annualized_return = returns.mean() * 12
    annualized_volatility = returns.std(ddof=1) * np.sqrt(12)

    if annualized_volatility == 0 or pd.isna(annualized_volatility):
        sharpe_ratio = np.nan
    else:
        sharpe_ratio = (excess_returns.mean() * 12) / annualized_volatility

    return {
        "annualized_return": annualized_return,
        "annualized_volatility": annualized_volatility,
        "sharpe_ratio": sharpe_ratio,
        "min_monthly_return": returns.min(),
        "max_monthly_return": returns.max(),
    }


def load_risk_free_rate() -> pd.DataFrame:
    """
    Je charge uniquement le taux sans risque brut, qui reste autorise.
    """
    risk_free = pd.read_excel(RAW_DIR / "Risk_Free_Rate_2025.xlsx")
    first_column = risk_free.columns[0]
    risk_free = risk_free.rename(columns={first_column: "yyyymm", "RF": "rf_percent"})
    risk_free["yyyymm"] = risk_free["yyyymm"].astype(str).str.strip()
    risk_free["date"] = pd.to_datetime(risk_free["yyyymm"] + "01", format="%Y%m%d", errors="coerce")
    risk_free["date"] = risk_free["date"] + pd.offsets.MonthEnd(0)
    risk_free["rf_decimal"] = pd.to_numeric(risk_free["rf_percent"], errors="coerce") / 100.0
    return risk_free[["date", "rf_decimal"]].dropna().copy()


def load_carbon_inputs() -> dict[str, object]:
    """
    Je charge seulement les fichiers utiles deja produits par la Part I.
    Je ne recharge aucun fichier Datastream brut.
    """
    annual_data = pd.read_excel(PROCESSED_DIR / "C_EM_Annual_Data.xlsx", parse_dates=["Delisting Date"])
    investment_set = pd.read_excel(PROCESSED_DIR / "F_MinVar_2_1_Investment_Set.xlsx", parse_dates=["delisting_date"])
    mv_weights = pd.read_excel(PROCESSED_DIR / "J_MinVar_2_2_Weights.xlsx")
    mv_performance = pd.read_excel(PROCESSED_DIR / "K_MinVar_2_2_Monthly_Performance.xlsx", parse_dates=["date"])
    vw_monthly_weights = pd.read_excel(
        PROCESSED_DIR / "M_ValueWeighted_2_3_Monthly_Weights.xlsx",
        parse_dates=["date", "rebalance_reference_date"],
    )
    vw_performance = pd.read_excel(
        PROCESSED_DIR / "N_ValueWeighted_2_3_Monthly_Performance.xlsx",
        parse_dates=["date", "rebalance_reference_date"],
    )
    monthly_data = pd.read_excel(PROCESSED_DIR / "B_EM_Monthly_Data.xlsx", parse_dates=["Date", "Delisting Date"])

    annual_data = annual_data.rename(
        columns={
            "ISIN": "isin",
            "Company Name": "company_name",
            "Country": "country",
            "Region": "region",
            "Delisting Date": "delisting_date",
            "Year": "year",
            "Scope 1 CO2": "scope1_co2",
            "Revenue USD": "revenue_usd",
            "Year End Market Value USD": "year_end_market_value_usd",
            "Year End Return Index": "year_end_return_index",
            "Price Available End Of Year": "price_available_eoy",
        }
    )
    monthly_data = monthly_data.rename(
        columns={
            "ISIN": "isin",
            "Company Name": "company_name",
            "Country": "country",
            "Region": "region",
            "Delisting Date": "delisting_date",
            "Date": "date",
            "Market Value USD": "market_value_usd",
            "Return Index": "return_index",
            "Monthly Return": "monthly_return",
            "Is Delisting Month": "is_delisting_month",
        }
    )

    for df in [annual_data, investment_set, mv_weights, vw_monthly_weights, monthly_data]:
        if "isin" in df.columns:
            df["isin"] = standardize_isin(df["isin"])

    monthly_data["date"] = pd.to_datetime(monthly_data["date"]) + pd.offsets.MonthEnd(0)
    mv_performance["date"] = pd.to_datetime(mv_performance["date"])
    vw_monthly_weights["date"] = pd.to_datetime(vw_monthly_weights["date"])
    vw_monthly_weights["rebalance_reference_date"] = pd.to_datetime(vw_monthly_weights["rebalance_reference_date"])
    vw_performance["date"] = pd.to_datetime(vw_performance["date"])
    vw_performance["rebalance_reference_date"] = pd.to_datetime(vw_performance["rebalance_reference_date"])

    covariance_workbook = pd.ExcelFile(PROCESSED_DIR / "H_MinVar_2_1_Covariance_Matrices.xlsx")
    covariance_matrices: dict[int, pd.DataFrame] = {}
    for sheet_name in covariance_workbook.sheet_names:
        formation_year = int(sheet_name.replace("Y_", ""))
        covariance_matrix = pd.read_excel(
            PROCESSED_DIR / "H_MinVar_2_1_Covariance_Matrices.xlsx",
            sheet_name=sheet_name,
            index_col=0,
        )
        covariance_matrix.index = covariance_matrix.index.astype(str).str.strip()
        covariance_matrix.columns = covariance_matrix.columns.astype(str).str.strip()
        covariance_matrices[formation_year] = covariance_matrix

    return {
        "annual_data": annual_data,
        "investment_set": investment_set,
        "monthly_data": monthly_data,
        "covariance_matrices": covariance_matrices,
        "mv_weights": mv_weights,
        "mv_performance": mv_performance,
        "vw_monthly_weights": vw_monthly_weights,
        "vw_performance": vw_performance,
        "risk_free": load_risk_free_rate(),
    }


def build_return_matrix(monthly_data: pd.DataFrame) -> pd.DataFrame:
    """Je pivote les rendements mensuels pour les calculs de portefeuille."""
    return_matrix = monthly_data.pivot(index="date", columns="isin", values="monthly_return")
    return_matrix.columns = return_matrix.columns.astype(str)
    return return_matrix.sort_index()


def get_vw_rebalancing_weights(vw_monthly_weights: pd.DataFrame) -> pd.DataFrame:
    """
    Je recupere uniquement les poids de janvier de chaque annee d'investissement.
    Ce sont eux qui representent les poids de rebalancement du benchmark VW.
    """
    january_weights = vw_monthly_weights.loc[vw_monthly_weights["date"].dt.month == 1].copy()
    first_date_per_year = january_weights.groupby("formation_year")["date"].min().rename("first_january_date")
    january_weights = january_weights.merge(first_date_per_year, on="formation_year", how="left")
    january_weights = january_weights.loc[january_weights["date"] == january_weights["first_january_date"]].copy()
    january_weights = january_weights.drop(columns=["first_january_date"])
    return january_weights.reset_index(drop=True)


def prepare_eligible_annual_panel(annual_data: pd.DataFrame, investment_set: pd.DataFrame) -> pd.DataFrame:
    """
    Je prepare le panel annuel restreint aux firmes eligibles et avec les variables carbone.
    """
    eligible = investment_set.loc[investment_set["min_var_eligible"]].copy()
    eligible = eligible.drop(
        columns=[
            "scope1_co2",
            "revenue_usd",
            "year_end_market_value_usd",
        ],
        errors="ignore",
    )
    annual_subset = annual_data[
        [
            "isin",
            "year",
            "scope1_co2",
            "revenue_usd",
            "year_end_market_value_usd",
        ]
    ].copy()

    annual_subset["revenue_musd"] = annual_subset["revenue_usd"] / 1000.0
    annual_subset["carbon_intensity"] = np.where(
        annual_subset["revenue_musd"] > 0,
        annual_subset["scope1_co2"] / annual_subset["revenue_musd"],
        np.nan,
    )
    annual_subset["e_over_cap"] = np.where(
        annual_subset["year_end_market_value_usd"] > 0,
        annual_subset["scope1_co2"] / annual_subset["year_end_market_value_usd"],
        np.nan,
    )

    eligible_annual = eligible.merge(
        annual_subset,
        left_on=["isin", "formation_year"],
        right_on=["isin", "year"],
        how="left",
    )
    eligible_annual["valid_carbon_inputs"] = eligible_annual[
        ["scope1_co2", "revenue_usd", "year_end_market_value_usd", "carbon_intensity", "e_over_cap"]
    ].notna().all(axis=1)

    return eligible_annual


def compute_portfolio_annual_carbon_metrics(
    weights: pd.DataFrame,
    eligible_annual: pd.DataFrame,
    portfolio_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Je calcule les details firme par firme, puis les metriques annuelles WACI et CF.
    """
    merged = weights.merge(
        eligible_annual,
        on=["isin", "formation_year", "investment_year"],
        how="inner",
    )
    merged = merged.loc[merged["valid_carbon_inputs"]].copy()

    for column in ["company_name", "country", "region"]:
        left_column = f"{column}_x"
        right_column = f"{column}_y"
        if left_column in merged.columns and right_column in merged.columns:
            merged[column] = merged[left_column].combine_first(merged[right_column])
        elif left_column in merged.columns:
            merged[column] = merged[left_column]
        elif right_column in merged.columns:
            merged[column] = merged[right_column]

    merged["waci_contribution"] = merged["weight"] * merged["carbon_intensity"]
    merged["cf_contribution"] = merged["weight"] * merged["e_over_cap"]
    merged["portfolio"] = portfolio_name

    annual_metrics = (
        merged.groupby(["portfolio", "formation_year", "investment_year"], as_index=False)
        .agg(
            waci=("waci_contribution", "sum"),
            cf=("cf_contribution", "sum"),
            covered_weight=("weight", "sum"),
            firm_count=("isin", "nunique"),
        )
        .sort_values("formation_year")
        .reset_index(drop=True)
    )

    top10 = (
        merged.sort_values(["formation_year", "waci_contribution"], ascending=[True, False])
        .groupby("formation_year")
        .head(10)
        .loc[
            :,
            [
                "portfolio",
                "formation_year",
                "investment_year",
                "isin",
                "company_name",
                "country",
                "region",
                "weight",
                "carbon_intensity",
                "waci_contribution",
            ],
        ]
        .rename(
            columns={
                "carbon_intensity": "ci_tonnes_per_musd_revenue",
                "waci_contribution": "contribution_to_waci",
            }
        )
        .reset_index(drop=True)
    )

    return merged, annual_metrics, top10


def compute_annual_wealth_path(monthly_performance: pd.DataFrame, initial_wealth: float = INITIAL_WEALTH_USD) -> pd.DataFrame:
    """
    Je reconstruis la valeur du portefeuille a chaque date de formation.
    """
    annual_growth = (
        monthly_performance.groupby(["formation_year", "investment_year"], as_index=False)["portfolio_return"]
        .apply(lambda returns: float((1 + returns).prod()))
        .rename(columns={"portfolio_return": "gross_return_factor"})
    )

    wealth_rows: list[dict[str, float]] = []
    current_wealth = initial_wealth

    for formation_year in range(FIRST_FORMATION_YEAR, LAST_FORMATION_YEAR + 1):
        investment_year = formation_year + 1
        wealth_rows.append(
            {
                "formation_year": formation_year,
                "investment_year": investment_year,
                "formation_wealth_usd": current_wealth,
            }
        )
        growth_row = annual_growth.loc[annual_growth["formation_year"] == formation_year]
        if not growth_row.empty:
            current_wealth *= float(growth_row["gross_return_factor"].iloc[0])

    return pd.DataFrame(wealth_rows)


def align_covariance_universe(covariance_matrix: pd.DataFrame, universe: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Je garde seulement les firmes presentes dans la matrice de covariance et sans ligne/colonne incomplete.
    """
    candidate_isins = [
        isin
        for isin in universe["isin"].astype(str).tolist()
        if isin in covariance_matrix.index and isin in covariance_matrix.columns
    ]
    covariance_matrix = covariance_matrix.loc[candidate_isins, candidate_isins].copy()

    valid_assets = covariance_matrix.index[
        covariance_matrix.notna().all(axis=1) & covariance_matrix.notna().all(axis=0)
    ].tolist()
    covariance_matrix = covariance_matrix.loc[valid_assets, valid_assets]

    universe = universe.loc[universe["isin"].isin(valid_assets)].copy()
    universe = universe.set_index("isin").loc[valid_assets].reset_index()

    return covariance_matrix, universe


def solve_quadratic_portfolio(
    covariance_matrix: pd.DataFrame,
    objective: str,
    benchmark_weights: pd.Series | None = None,
    carbon_vector: pd.Series | None = None,
    carbon_target: float | None = None,
    maxiter: int = 2000,
    ftol: float = 1e-12,
) -> tuple[pd.Series, dict[str, object]]:
    """
    Je resous un probleme quadratique de portefeuille avec SLSQP.
    """
    asset_names = covariance_matrix.columns.tolist()
    sigma = covariance_matrix.to_numpy(dtype=float)
    asset_count = len(asset_names)

    if objective == "tracking_error":
        if benchmark_weights is None:
            raise ValueError("benchmark_weights is required for tracking_error objective")
        benchmark = benchmark_weights.loc[asset_names].to_numpy(dtype=float)
        initial_weights = benchmark.copy()
    else:
        benchmark = None
        initial_weights = np.repeat(1.0 / asset_count, asset_count)

    bounds = [(0.0, 1.0)] * asset_count
    constraints = [{"type": "eq", "fun": lambda weights: np.sum(weights) - 1.0}]

    carbon_array = None
    if carbon_vector is not None:
        carbon_array = carbon_vector.loc[asset_names].to_numpy(dtype=float)
    if carbon_array is not None and carbon_target is not None:
        constraints.append(
            {"type": "ineq", "fun": lambda weights: float(carbon_target - np.dot(weights, carbon_array))}
        )

    def objective_function(weights: np.ndarray) -> float:
        if objective == "tracking_error":
            diff = weights - benchmark
            return float(diff.T @ sigma @ diff)
        return float(weights.T @ sigma @ weights)

    optimization = minimize(
        objective_function,
        x0=initial_weights,
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={"maxiter": maxiter, "ftol": ftol},
    )

    raw_weights = optimization.x if optimization.success else initial_weights
    clipped_weights = np.clip(raw_weights, 0.0, 1.0)
    if clipped_weights.sum() > 0:
        clipped_weights = clipped_weights / clipped_weights.sum()

    weights = pd.Series(clipped_weights, index=asset_names, name="weight")
    achieved_cf = np.nan
    if carbon_array is not None:
        achieved_cf = float(np.dot(weights.to_numpy(dtype=float), carbon_array))

    success = bool(optimization.success)
    if carbon_target is not None and pd.notna(achieved_cf):
        success = success and achieved_cf <= carbon_target + 1e-8

    info = {
        "success": success,
        "scipy_success": bool(optimization.success),
        "status": int(optimization.status),
        "message": str(optimization.message),
        "achieved_cf": achieved_cf,
    }
    return weights, info


def build_drifted_performance(return_matrix: pd.DataFrame, annual_weights: pd.DataFrame) -> pd.DataFrame:
    """
    Je calcule les rendements ex post en laissant les poids deriver dans l'annee.
    """
    portfolio_rows: list[dict[str, object]] = []

    for formation_year in range(FIRST_FORMATION_YEAR, LAST_FORMATION_YEAR + 1):
        year_weights = annual_weights.loc[annual_weights["formation_year"] == formation_year].copy()
        if year_weights.empty:
            continue

        current_weights = year_weights.set_index("isin")["weight"].astype(float).copy()
        investment_year = formation_year + 1
        investment_dates = pd.date_range(
            start=pd.Timestamp(investment_year, 1, 31),
            end=pd.Timestamp(investment_year, 12, 31),
            freq="M",
        )

        for date in investment_dates:
            if date not in return_matrix.index:
                continue

            month_returns = return_matrix.loc[date, current_weights.index].fillna(0.0)
            portfolio_return = float((current_weights * month_returns).sum())

            portfolio_rows.append(
                {
                    "date": date,
                    "formation_year": formation_year,
                    "investment_year": investment_year,
                    "portfolio_return": portfolio_return,
                }
            )

            gross_asset_returns = 1.0 + month_returns
            gross_portfolio_return = 1.0 + portfolio_return

            if gross_portfolio_return == 0:
                current_weights = current_weights * 0.0
            else:
                current_weights = current_weights * gross_asset_returns / gross_portfolio_return
                if current_weights.sum() > 0:
                    current_weights = current_weights / current_weights.sum()

    performance = pd.DataFrame(portfolio_rows).sort_values("date").reset_index(drop=True)
    performance["cumulative_growth"] = (1 + performance["portfolio_return"]).cumprod()
    return performance


def compare_weight_structures(
    base_weights: pd.DataFrame,
    new_weights: pd.DataFrame,
    base_label: str,
    new_label: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Je compare deux structures de poids pour decrire les changements de portefeuille.
    """
    comparison = base_weights.merge(
        new_weights,
        on=["isin", "formation_year", "investment_year"],
        how="outer",
        suffixes=(f"_{base_label}", f"_{new_label}"),
    )

    for column in ["company_name", "country", "region"]:
        left = f"{column}_{base_label}"
        right = f"{column}_{new_label}"
        if left in comparison.columns and right in comparison.columns:
            comparison[column] = comparison[left].combine_first(comparison[right])
        elif left in comparison.columns:
            comparison[column] = comparison[left]
        elif right in comparison.columns:
            comparison[column] = comparison[right]

    comparison[f"weight_{base_label}"] = comparison[f"weight_{base_label}"].fillna(0.0)
    comparison[f"weight_{new_label}"] = comparison[f"weight_{new_label}"].fillna(0.0)
    comparison["weight_change"] = comparison[f"weight_{new_label}"] - comparison[f"weight_{base_label}"]

    excluded = comparison.loc[
        (comparison[f"weight_{base_label}"] > 0) & (comparison[f"weight_{new_label}"] <= 1e-12)
    ].copy()
    overweighted = comparison.loc[comparison["weight_change"] > 1e-12].copy()
    overweighted = overweighted.sort_values(["formation_year", "weight_change"], ascending=[True, False])

    country_shift = (
        comparison.groupby(["formation_year", "country"], as_index=False)[[f"weight_{base_label}", f"weight_{new_label}"]]
        .sum()
    )
    country_shift["weight_shift"] = country_shift[f"weight_{new_label}"] - country_shift[f"weight_{base_label}"]

    return comparison, excluded, overweighted, country_shift


def build_year_end_vw_benchmark_weights(
    eligible_annual: pd.DataFrame,
    require_valid_carbon: bool = False,
) -> pd.DataFrame:
    """
    Je construis les poids annuels du benchmark VW a partir des market caps de fin d'annee.
    """
    benchmark_rows: list[pd.DataFrame] = []

    for formation_year in range(FIRST_FORMATION_YEAR, LAST_FORMATION_YEAR + 1):
        year_slice = eligible_annual.loc[eligible_annual["formation_year"] == formation_year].copy()
        if require_valid_carbon:
            year_slice = year_slice.loc[year_slice["valid_carbon_inputs"]].copy()
        if year_slice.empty:
            continue

        total_market_cap = year_slice["year_end_market_value_usd"].sum()
        if total_market_cap <= 0 or pd.isna(total_market_cap):
            continue

        year_slice["weight"] = year_slice["year_end_market_value_usd"] / total_market_cap
        year_slice = year_slice[
            [
                "isin",
                "company_name",
                "country",
                "region",
                "formation_year",
                "investment_year",
                "weight",
            ]
        ].copy()
        benchmark_rows.append(year_slice)

    return pd.concat(benchmark_rows, ignore_index=True)


def build_reference_summary_table(mv_performance: pd.DataFrame, vw_performance: pd.DataFrame, risk_free: pd.DataFrame) -> pd.DataFrame:
    """
    Je reconstruis les statistiques de la Part I pour controler que tout colle.
    """
    risk_free_series = risk_free.set_index("date")["rf_decimal"]

    mv_stats = compute_summary_stats(
        mv_performance.set_index("date")["portfolio_return"],
        risk_free_series,
    )
    vw_stats = compute_summary_stats(
        vw_performance.set_index("date")["portfolio_return"],
        risk_free_series,
    )

    reference_rows = [
        {"portfolio": "mv_oos", "metric": "annualized_return", "calculated": mv_stats["annualized_return"], "reference": 0.079043},
        {"portfolio": "mv_oos", "metric": "annualized_volatility", "calculated": mv_stats["annualized_volatility"], "reference": 0.102693},
        {"portfolio": "mv_oos", "metric": "sharpe_ratio", "calculated": mv_stats["sharpe_ratio"], "reference": 0.595075},
        {"portfolio": "mv_oos", "metric": "min_monthly_return", "calculated": mv_stats["min_monthly_return"], "reference": -0.067042},
        {"portfolio": "mv_oos", "metric": "max_monthly_return", "calculated": mv_stats["max_monthly_return"], "reference": 0.140324},
        {"portfolio": "vw", "metric": "annualized_return", "calculated": vw_stats["annualized_return"], "reference": 0.086513},
        {"portfolio": "vw", "metric": "annualized_volatility", "calculated": vw_stats["annualized_volatility"], "reference": 0.156870},
        {"portfolio": "vw", "metric": "sharpe_ratio", "calculated": vw_stats["sharpe_ratio"], "reference": 0.440095},
        {"portfolio": "vw", "metric": "min_monthly_return", "calculated": vw_stats["min_monthly_return"], "reference": -0.167018},
        {"portfolio": "vw", "metric": "max_monthly_return", "calculated": vw_stats["max_monthly_return"], "reference": 0.134429},
    ]

    reference_table = pd.DataFrame(reference_rows)
    reference_table["difference"] = reference_table["calculated"] - reference_table["reference"]
    reference_table["within_tolerance"] = reference_table["difference"].abs() <= 0.001
    return reference_table


def ensure_part1_cross_check(reference_table: pd.DataFrame) -> None:
    """
    Je bloque la suite si les resultats de la Part I ne collent pas.
    """
    if not reference_table["within_tolerance"].all():
        raise ValueError("Part I cross-check failed. The current outputs do not match the validated reference values.")


def build_note_table(note_text: str) -> pd.DataFrame:
    """Je cree une petite table de note pour les sorties qualitatives."""
    return pd.DataFrame([{"note": note_text}])


def build_caption_table(captions: list[dict[str, str]]) -> pd.DataFrame:
    """Je rassemble les explications des tableaux et figures dans une feuille simple."""
    return pd.DataFrame(captions)


def warn(message: str) -> None:
    """Je journalise les avertissements sans arreter l'execution."""
    warnings.warn(message)
    print(f"WARNING: {message}", flush=True)
