import duckdb, time
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────
PARQUET_PATH = Path("data/raw/export-detail-bilan.parquet")
DB_PATH      = Path("data/finscope.duckdb")


def load_raw_bilans(force: bool = False) -> None:
    """
    Charge le parquet Signaux Faibles dans DuckDB.
    Crée la table raw_bilans dans data/finscope.duckdb.

    Args:
        force: Si True, recrée la table même si elle existe déjà.
    """
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(
            f"Parquet introuvable : {PARQUET_PATH}\n"
            f"→ Lancer d'abord ingestion/download_sf.py"
        )

    con = duckdb.connect(str(DB_PATH))

    # Vérifier si la table existe déjà
    tables = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='raw_bilans'"
    ).fetchall()

    if tables and not force:
        n = con.execute("SELECT COUNT(*) FROM raw_bilans").fetchone()[0]
        print(f"[CACHE] raw_bilans existe déjà ({n:,} lignes). Passer force=True pour recharger.")
        con.close()
        return

    print(f"[LOAD] Chargement {PARQUET_PATH} → {DB_PATH}")
    t0 = time.time()

    # Supprimer si force reload
    if force:
        con.execute("DROP TABLE IF EXISTS raw_bilans")

    # Charger le parquet comme table persistée
    con.execute(f"""
        CREATE TABLE raw_bilans AS
        SELECT * FROM read_parquet('{PARQUET_PATH}')
    """)

    elapsed = time.time() - t0
    n = con.execute("SELECT COUNT(*) FROM raw_bilans").fetchone()[0]

    print(f"[OK] {n:,} lignes chargées en {elapsed:.1f}s")
    con.close()


def explore() -> None:
    """Exploration de base : schema, stats, années, secteurs."""
    con = duckdb.connect(str(DB_PATH))

    print("\n" + "="*60)
    print("EXPLORATION raw_bilans")
    print("="*60)

    # ── 1. Schema complet ────────────────────────────────────────
    print("\n[1] SCHEMA :")
    schema = con.execute("DESCRIBE raw_bilans").df()
    print(schema.to_string(index=False))

    # ── 2. Volume ────────────────────────────────────────────────
    print("\n[2] VOLUME :")
    n_total = con.execute("SELECT COUNT(*) FROM raw_bilans").fetchone()[0]
    n_siren = con.execute("SELECT COUNT(DISTINCT siren) FROM raw_bilans").fetchone()[0]
    print(f"  Lignes totales : {n_total:,}")
    print(f"  SIREN uniques  : {n_siren:,}")

    # ── 3. Années disponibles ────────────────────────────────────
    print("\n[3] ANNÉES :")
    # Adapter le nom de colonne selon ce que DESCRIBE retourne
    for col_annee in ["annee", "exercice", "date_cloture", "year"]:
        try:
            years = con.execute(f"""
                SELECT {col_annee}, COUNT(*) as nb
                FROM raw_bilans
                GROUP BY {col_annee}
                ORDER BY {col_annee}
            """).df()
            print(years.to_string(index=False))
            break
        except:
            continue

    # ── 4. Taux de nulls colonnes financières ────────────────────
    print("\n[4] TAUX DE NULLS (colonnes financières) :")
    cols_finance = [
        "ca", "chiffre_affaires", "resultat_net", "capitaux_propres",
        "total_actif", "dettes", "stocks", "amortissements"
    ]
    # Filtrer sur les colonnes réellement présentes
    cols_presentes = schema["column_name"].tolist()
    cols_a_tester = [c for c in cols_finance if c in cols_presentes]

    if cols_a_tester:
        null_q = ", ".join(
            [f"ROUND(100.0 * SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS null_pct_{c}"
             for c in cols_a_tester]
        )
        nulls = con.execute(f"SELECT {null_q} FROM raw_bilans").df()
        for col in nulls.columns:
            print(f"  {col.replace('null_pct_', ''):<30} {nulls[col].values[0]}% null")

    # ── 5. Top secteurs NAF ──────────────────────────────────────
    print("\n[5] TOP 10 SECTEURS NAF :")
    for col_naf in ["code_naf", "naf", "ape", "code_ape"]:
        try:
            naf = con.execute(f"""
                SELECT {col_naf}, COUNT(*) as nb
                FROM raw_bilans
                GROUP BY {col_naf}
                ORDER BY nb DESC
                LIMIT 10
            """).df()
            print(naf.to_string(index=False))
            break
        except:
            continue

    con.close()
    print("\n[DONE] Exploration terminée — noter les vrais noms de colonnes pour dbt Day 04")


# ── MAIN ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== FinScope Day 02 — Load parquet → DuckDB ===")
    load_raw_bilans()
    explore()