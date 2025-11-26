"""
ETL pipeline completo para o dataset 'AI_Impact_on_Jobs_2030.csv'
Salva arquivo enriquecido e base SQLite.
"""

import os
import argparse
import json
from typing import Tuple
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

############################
# EXTRACT
############################

def extract(input_path: str) -> pd.DataFrame:
    """Carrega o CSV e realiza checagens iniciais."""
    df = pd.read_csv(input_path)

    print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
    print(df.dtypes)
    print("Missing per column:", df.isna().sum())

    return df

############################
# TRANSFORM HELPERS
############################

def _standardize_job_title(s: str) -> str:
    if pd.isna(s):
        return s
    return s.strip().lower()


def _experience_level(years: float) -> str:
    try:
        y = float(years)
    except Exception:
        return "unknown"

    if y < 3:
        return "Junior"
    if y < 7:
        return "Mid"
    if y < 15:
        return "Senior"
    return "Expert"


def _salary_zscore(series: pd.Series) -> pd.Series:
    return (series - series.mean()) / series.std(ddof=0)


############################
# TRANSFORM
############################

def transform(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    df = df.copy()

    # --- 1) Normalização dos nomes dos empregos
    if 'Job_Title' in df.columns:
        df['Job_Title_clean'] = df['Job_Title'].astype(str).map(_standardize_job_title)

    # --- 2) Tipos numéricos garantidos
    numeric_cols = [
        'Average_Salary', 'Years_Experience', 'AI_Exposure_Index',
        'Tech_Growth_Factor', 'Automation_Probability_2030'
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # --- 3) Experience Level
    if 'Years_Experience' in df.columns:
        df['Experience_Level'] = df['Years_Experience'].apply(_experience_level)

    # --- 4) Normalizações salariais
    if 'Average_Salary' in df.columns:
        df['Salary_ZScore'] = _salary_zscore(df['Average_Salary'])
        df['Salary_Percentile'] = df['Average_Salary'].rank(pct=True)

    # --- 5) Skills
    skill_cols = [c for c in df.columns if c.lower().startswith('skill_')]

    if skill_cols:
        df[skill_cols] = df[skill_cols].apply(pd.to_numeric, errors='coerce')

        df['Top_Skill'] = df[skill_cols].idxmax(axis=1)
        df['Skill_Strength_Avg'] = df[skill_cols].mean(axis=1)
        df['Skill_Strength_Std'] = df[skill_cols].std(axis=1)
        df['Skill_Vector_Magnitude'] = np.sqrt((df[skill_cols] ** 2).sum(axis=1))

    # --- 6) Vulnerability Index (normalizado + ponderado)
    def _safe_norm(series):
        if series.max() == series.min():
            return series.fillna(0)
        return (series - series.min()) / (series.max() - series.min())

    auto_norm = _safe_norm(df['Automation_Probability_2030']) if 'Automation_Probability_2030' in df else 0
    expo_norm = _safe_norm(df['AI_Exposure_Index']) if 'AI_Exposure_Index' in df else 0
    tech_norm = _safe_norm(df['Tech_Growth_Factor']) if 'Tech_Growth_Factor' in df else 0

    df['Vulnerability_Index'] = (
        0.5 * auto_norm +
        0.3 * expo_norm +
        0.2 * tech_norm
    )

    # --- 7) Indicador de Substituição por IA
    human_cols = [c for c in ['Skill_7', 'Skill_9', 'Skill_10'] if c in df.columns]

    if human_cols:
        df['Human_Skill_Index'] = df[human_cols].mean(axis=1)
    else:
        df['Human_Skill_Index'] = 0

    base = df['Automation_Probability_2030'].fillna(0)
    human = df['Human_Skill_Index'].fillna(0)
    skill_avg = df['Skill_Strength_Avg'].fillna(0)

    df['Automation_Replacement_Index_raw'] = base - 0.3 * human - 0.1 * skill_avg
    df['Automation_Replacement_Index'] = df['Automation_Replacement_Index_raw'].clip(0, 1)

    # --- 8) Classificação Quantitativa
    def _risk_label(x):
        if x >= 0.7:
            return 'High'
        if x >= 0.4:
            return 'Medium'
        return 'Low'

    df['Risk_Category_Quant'] = df['Automation_Replacement_Index'].apply(_risk_label)

    # --- 9) Inconsistências
    if 'Risk_Category' in df.columns:
        df['Inconsistency_Flag'] = (
            df['Risk_Category'].astype(str).str.lower().str.strip() !=
            df['Risk_Category_Quant'].astype(str).str.lower().str.strip()
        )
    else:
        df['Inconsistency_Flag'] = False

    # --- 10) Relatório de Transformação
    report = {
        "rows": len(df),
        "cols": len(df.columns),
        "missing_total": int(df.isna().sum().sum()),
        "skill_columns": skill_cols,
        "human_skill_columns": human_cols
    }

    return df, report

############################
# LOAD
############################

def load(df: pd.DataFrame, out_dir: str, table_name: str = 'jobs'):
    os.makedirs(out_dir, exist_ok=True)

    csv_path = os.path.join(out_dir, 'enriched_jobs.csv')
    db_path = os.path.join(out_dir, 'enriched_jobs.db')

    df.to_csv(csv_path, index=False)
    print(f"CSV salvo em {csv_path}")

    try:
        engine = create_engine(f'sqlite:///{db_path}')
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"SQLite salvo em {db_path} (tabela: {table_name})")
    except Exception as e:
        print("Erro ao salvar SQLite:", e)

    return csv_path, db_path

############################
# TRANSFORM REPORT
############################

def generate_transform_report(report: dict, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)

    path = os.path.join(out_dir, 'transform_report.md')
    with open(path, 'w', encoding='utf-8') as f:
        f.write('# Relatório de Transformação')
        f.write('```json')
        f.write(json.dumps(report, indent=2))
        f.write('```')

    print(f"Relatório salvo em {path}")
    return path

############################
# MAIN
############################

def main(args=None):
    parser = argparse.ArgumentParser(description='Pipeline ETL — Impacto da IA nos Empregos 2030')
    parser.add_argument('--input', required=True, help='Caminho do CSV de entrada')
    parser.add_argument('--out_dir', default='outputs', help='Diretório de saída')

    opts = parser.parse_args(args=args)

    df = extract(opts.input)
    df_enriched, summary = transform(df)
    load(df_enriched, opts.out_dir)
    generate_transform_report(summary, opts.out_dir)

    print("ETL concluído com sucesso!")


if __name__ == '__main__':
    main()
