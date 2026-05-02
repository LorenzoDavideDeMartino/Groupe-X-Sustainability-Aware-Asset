# Groupe-X-Sustainability-Aware-Asset

Emerging Markets / Scope 1

## Project Structure

```text
saam-carbon-portfolio/
├── data/
│   ├── Raw/
│   │   ├── Static_2025.xlsx
│   │   ├── DS_CO2_SCOPE_1_Y_2025.xlsx
│   │   ├── DS_CO2_SCOPE_2_Y_2025.xlsx
│   │   ├── DS_REV_Y_2025.xlsx
│   │   ├── DS_MV_T_USD_M_2025.xlsx
│   │   ├── DS_MV_T_USD_Y_2025.xlsx
│   │   ├── DS_RI_T_USD_M_2025.xlsx
│   │   ├── DS_RI_T_USD_Y_2025.xlsx
│   │   └── Risk_Free_Rate_2025.xlsx
│   └── processed/
│       ├── A_EM_Companies.xlsx
│       ├── B_EM_Monthly_Data.xlsx
│       ├── C_EM_Annual_Data.xlsx
│       ├── D_EM_Base_Investment_Set.xlsx
│       ├── F_MinVar_2_1_Investment_Set.xlsx
│       ├── G_MinVar_2_1_Expected_Returns.xlsx
│       ├── H_MinVar_2_1_Covariance_Matrices.xlsx
│       ├── I_MinVar_2_1_Summary.xlsx
│       ├── J_MinVar_2_2_Weights.xlsx
│       ├── K_MinVar_2_2_Monthly_Performance.xlsx
│       ├── L_MinVar_2_2_Summary.xlsx
│       ├── M_ValueWeighted_2_3_Monthly_Weights.xlsx
│       ├── N_ValueWeighted_2_3_Monthly_Performance.xlsx
│       └── O_ValueWeighted_2_3_Summary.xlsx
├── src/
│   ├── data_cleaning_part1.py
│   ├── minimum_variance_part2.py
│   ├── minimum_variance_part2_2.py
│   ├── value_weighted_part2_3.py
│   └── pipeline.py
├── .gitignore
└── README.md
```

## Current Scripts

- `data_cleaning_part1.py`: cleans the raw Datastream files and creates the base EM outputs.
- `minimum_variance_part2.py`: builds the investment set for section 2.1 and computes expected returns and covariance matrices.
- `minimum_variance_part2_2.py`: computes the long-only minimum-variance portfolio for section 2.2.
- `value_weighted_part2_3.py`: computes the value-weighted benchmark for section 2.3.
- `pipeline.py`: runs the current project pipeline in the correct order.

## Run The Pipeline

From the project root:

```powershell
python src\pipeline.py
```

At the moment, this pipeline runs the current Part I workflow. It can be extended later when the next project parts are added.
