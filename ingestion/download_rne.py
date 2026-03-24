"""
FinScope — Day 09
Chargement StockUniteLegale Sirene INSEE depuis data.gouv.fr
URL stable parquet : https://www.data.gouv.fr/api/1/datasets/r/350182c9-148a-46e0-8389-76c2ec1374a3
Colonne NAF : activitePrincipaleUniteLegale
Couverture : 100% des SIRENs de raw_bilans
"""

import duckdb
from pathlib import Path

DB_PATH = Path("data/finscope.duckdb")

SIRENE_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/"
    "350182c9-148a-46e0-8389-76c2ec1374a3"
)

def load():
    print("[LOAD] StockUniteLegale Sirene INSEE → raw_entreprises...")
    print("       DuckDB stream depuis URL — pas de fichier local")

    con = duckdb.connect(str(DB_PATH))
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_entreprises AS
        SELECT
            siren,
            activitePrincipaleUniteLegale  AS code_naf,
            denominationUniteLegale        AS denomination,
            categorieEntreprise            AS categorie
        FROM read_parquet('{SIRENE_URL}')
        WHERE siren IS NOT NULL
          AND activitePrincipaleUniteLegale IS NOT NULL
          AND TRIM(activitePrincipaleUniteLegale) != ''
    """)

    n = con.execute("SELECT COUNT(*) FROM raw_entreprises").fetchone()[0]
    print(f"[OK] raw_entreprises : {n:,} lignes")
    print(con.execute("""
        SELECT code_naf, COUNT(*) as nb
        FROM raw_entreprises
        GROUP BY code_naf ORDER BY nb DESC LIMIT 5
    """).df().to_string(index=False))
    con.close()
    print("[DONE] Prêt pour dbt stg_entreprises")

if __name__ == "__main__":
    load()