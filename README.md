Projeto: ETL — Impacto da IA nos Empregos (2030)

Projeto de exemplo para demonstrar um pipeline ETL (Extract, Transform, Load) usando Python e Pandas. O dataset contém informações sobre impactos do uso de IA em empregos em 2030 (salários, níveis educacionais, probabilidade de automação, índices de exposição à IA, vetor de 10 skills por ocupação, etc.).

O objetivo é:

Demonstrar boas práticas de ETL

Enriquecer o dataset com novas features analíticas (incluindo um Indicador de Substituição por IA)

Preparar saídas para análise ou integração (CSV e SQLite)

Gerar artefatos úteis (dicionário de dados e relatório de transformação)

Estrutura de arquivos sugerida
project-root/
├─ data/
│  └─ AI_Impact_on_Jobs_2030.csv   # arquivo de entrada (fornecido)
├─ outputs/
│  ├─ enriched_jobs.csv
│  ├─ enriched_jobs.db
│  └─ transform_report.md
├─ etl_pipeline.py                 # script principal (copiar do bloco de código abaixo)
└─ README.md                       # este README

Requisitos

Python 3.9+
pandas
numpy
sqlalchemy (opcional, para SQLite)

Instalar dependências:
python -m pip install pandas numpy sqlalchemy

Passos ETL (descrição)

Extract
Carregar CSV (tratando encoding, tipos e parsing básicos)
Rodar checagens iniciais: info(), describe(), contagem de missing, duplicados

Transform

Limpeza: coerência de colunas, correção de tipos, normalização de textos
Feature engineering (exemplos aplicados neste projeto):
Vulnerability_Index (combinação ponderada de Automation_Probability_2030, AI_Exposure_Index, Tech_Growth_Factor)
Automation_Replacement_Index (o indicador pedido — ver implementação abaixo)
Experience_Level (faixas a partir de Years_Experience)
Salary_ZScore, Salary_Percentile
Top_Skill, Skill_Strength_Avg, Skill_Vector_Magnitude
Inconsistency_Flag (se a Risk_Category original divergir da classificação quantitativa)
Clustering ou grouping (opcional — não incluí k-means neste script, mas deixo ponto de extensão)

Load

Salvar CSV enriquecido em outputs/enriched_jobs.csv
Salvar em SQLite outputs/enriched_jobs.db tabela jobs
Gerar relatório de transformação simples outputs/transform_report.md

Como rodar
python etl_pipeline.py --input data/AI_Impact_on_Jobs_2030.csv --out_dir outputs/
