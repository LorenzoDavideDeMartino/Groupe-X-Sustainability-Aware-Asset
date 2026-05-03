# Groupe X - Sustainability Aware Asset Management

Emerging Markets / Scope 1

## Project Overview

This repository contains the full workflow for the SAAM group project.
It starts with Datastream data cleaning, then builds the standard portfolio allocation of Part I, and finally extends the analysis to carbon-constrained and net-zero portfolio allocation for Parts III and IV.

The project focuses on:
- Emerging Markets firms only
- Scope 1 carbon emissions only
- Long-only portfolio construction
- Annual rebalancing with monthly weight drift

## Project Structure

```text
saam-carbon-portfolio/
|-- data/
|   |-- Raw/
|   |   |-- Static_2025.xlsx
|   |   |-- DS_CO2_SCOPE_1_Y_2025.xlsx
|   |   |-- DS_CO2_SCOPE_2_Y_2025.xlsx
|   |   |-- DS_REV_Y_2025.xlsx
|   |   |-- DS_MV_T_USD_M_2025.xlsx
|   |   |-- DS_MV_T_USD_Y_2025.xlsx
|   |   |-- DS_RI_T_USD_M_2025.xlsx
|   |   |-- DS_RI_T_USD_Y_2025.xlsx
|   |   `-- Risk_Free_Rate_2025.xlsx
|   `-- processed/
|       |-- A_EM_Companies.xlsx
|       |-- B_EM_Monthly_Data.xlsx
|       |-- C_EM_Annual_Data.xlsx
|       |-- D_EM_Base_Investment_Set.xlsx
|       |-- F_MinVar_2_1_Investment_Set.xlsx
|       |-- G_MinVar_2_1_Expected_Returns.xlsx
|       |-- H_MinVar_2_1_Covariance_Matrices.xlsx
|       |-- I_MinVar_2_1_Summary.xlsx
|       |-- J_MinVar_2_2_Weights.xlsx
|       |-- K_MinVar_2_2_Monthly_Performance.xlsx
|       |-- L_MinVar_2_2_Summary.xlsx
|       |-- M_ValueWeighted_2_3_Monthly_Weights.xlsx
|       |-- N_ValueWeighted_2_3_Monthly_Performance.xlsx
|       |-- O_ValueWeighted_2_3_Summary.xlsx
|       |-- P_Carbon_3_1_WACI_CF.xlsx
|       |-- Q_MinVar_Carbon_3_2_Weights.xlsx
|       |-- R_MinVar_Carbon_3_2_Monthly_Performance.xlsx
|       |-- S_MinVar_Carbon_3_2_Summary.xlsx
|       |-- T_TrackingError_Carbon_3_3_Weights.xlsx
|       |-- U_TrackingError_Carbon_3_3_Monthly_Performance.xlsx
|       |-- V_TrackingError_Carbon_3_3_Summary.xlsx
|       |-- W_NetZero_4_1_Weights.xlsx
|       |-- X_NetZero_4_1_Monthly_Performance.xlsx
|       |-- Y_NetZero_4_1_Summary.xlsx
|       `-- AA_Passive_Comparison_4_2.xlsx
|-- src/
|   |-- data_cleaning_part1.py
|   |-- minimum_variance_part2.py
|   |-- minimum_variance_part2_2.py
|   |-- value_weighted_part2_3.py
|   |-- carbon_portfolio_utils.py
|   |-- carbon_footprint_part3_1.py
|   |-- minimum_variance_carbon_part3_2.py
|   |-- tracking_error_carbon_part3_3.py
|   |-- carbon_comparison_part3_4.py
|   |-- net_zero_part4_1.py
|   |-- passive_comparison_part4_2.py
|   |-- pipeline.py
|   `-- pipeline_carbon_only.py
|-- .gitignore
`-- README.md
```

## Main Scripts

### Part I

- `data_cleaning_part1.py`
  Cleans the raw Datastream files and builds the processed Emerging Markets datasets.

- `minimum_variance_part2.py`
  Builds the investment set for Section 2.1 and computes expected returns and covariance matrices.

- `minimum_variance_part2_2.py`
  Computes the long-only minimum-variance portfolio for Section 2.2.

- `value_weighted_part2_3.py`
  Computes the value-weighted benchmark portfolio for Section 2.3.

### Part III

- `carbon_footprint_part3_1.py`
  Computes the annual WACI and carbon footprint of the reference minimum-variance and value-weighted portfolios.

- `minimum_variance_carbon_part3_2.py`
  Builds the long-only minimum-variance portfolio under a 50% carbon-footprint reduction constraint.

- `tracking_error_carbon_part3_3.py`
  Builds the tracking-error-minimizing passive portfolio under a 50% carbon-footprint reduction constraint.

- `carbon_comparison_part3_4.py`
  Compares the four portfolios of Part III on financial and carbon metrics.

### Part IV

- `net_zero_part4_1.py`
  Builds the passive net-zero portfolio with a tightening carbon-footprint path based on the 2013 baseline.

- `passive_comparison_part4_2.py`
  Compares the passive benchmark, the 50% carbon-reduction passive portfolio, and the net-zero passive portfolio.

## Pipelines

### Full Pipeline

Runs the whole project workflow from data cleaning to the end of Part IV:

```powershell
python src\pipeline.py
```

### Carbon-Only Pipeline

Runs only the carbon allocation sections, starting from the already computed files in `data/processed`:

```powershell
python src\pipeline_carbon_only.py
```

## Notes

- The carbon sections use processed files only.
- Scope 1 is the only emissions scope used in the analysis.
- The project uses annual portfolio formation years from 2013 to 2024 and monthly out-of-sample returns from 2014 to 2025.
