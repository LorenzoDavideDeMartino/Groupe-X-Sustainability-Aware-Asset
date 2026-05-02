from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "Raw"

MONTHLY_MV_FILE = "DS_MV_T_USD_M_2025.xlsx"


def load_monthly_market_cap_long() -> pd.DataFrame:
    """
    Charge le fichier Datastream de market cap mensuelle et le met au format long.
    Sortie:
    - isin
    - date
    - market_cap
    """
    mv_raw = pd.read_excel(RAW_DIR / MONTHLY_MV_FILE, sheet_name="MV")

    mv_raw = mv_raw.loc[mv_raw["ISIN"].notna()].copy()

    id_cols = ["NAME", "ISIN"]
    date_cols = [col for col in mv_raw.columns if col not in id_cols]

    mv_long = mv_raw.melt(
        id_vars=id_cols,
        value_vars=date_cols,
        var_name="date",
        value_name="market_cap",
    )

    mv_long = mv_long.rename(columns={"ISIN": "isin", "NAME": "company_name_mv"})
    mv_long["isin"] = mv_long["isin"].astype(str).str.strip()
    mv_long["date"] = pd.to_datetime(mv_long["date"], errors="coerce")
    mv_long["date"] = mv_long["date"] + pd.offsets.MonthEnd(0)
    mv_long["market_cap"] = pd.to_numeric(mv_long["market_cap"], errors="coerce")

    mv_long.loc[mv_long["market_cap"] <= 0, "market_cap"] = np.nan

    mv_long = mv_long[["isin", "date", "market_cap"]].drop_duplicates()

    return mv_long


def merge_market_cap_into_monthly_data(monthly_data: pd.DataFrame) -> pd.DataFrame:
    """
    Ajoute la market cap mensuelle a monthly_data.
    """
    mv_long = load_monthly_market_cap_long()

    monthly_data = monthly_data.copy()
    monthly_data["isin"] = monthly_data["isin"].astype(str).str.strip()
    monthly_data["date"] = pd.to_datetime(monthly_data["date"], errors="coerce")
    monthly_data["date"] = monthly_data["date"] + pd.offsets.MonthEnd(0)

    monthly_data = monthly_data.merge(
        mv_long,
        on=["isin", "date"],
        how="left",
    )

    return monthly_data

if __name__ == "__main__":
    PROCESSED_DIR = BASE_DIR / "data" / "processed"

    monthly_data = pd.read_excel(
        PROCESSED_DIR / "B_EM_Monthly_Data.xlsx",
        parse_dates=["Date", "Delisting Date"],
    )

    monthly_data = monthly_data.rename(columns={
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
    })

    monthly_data_with_mc = merge_market_cap_into_monthly_data(monthly_data)

    output_path = PROCESSED_DIR / "B_EM_Monthly_Data_With_MC.xlsx"
    monthly_data_with_mc.to_excel(output_path, index=False)

    print(f"Fichier sauvegardé : {output_path}", flush=True)