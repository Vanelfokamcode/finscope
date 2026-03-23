import duckdb
from pathlib import Path

DB_PATH = Path("data/finscope.duckdb")

def section(title: str) -> None:
    print(f"\n{'='*60}\n{title}\n{'='*60}")

def run() -> None:
    con = duckdb.connect(str(DB_PATH))

    # ── 1. TAUX DE NULLS ─────────────────────────────────────────
    section("1. TAUX DE NULLS — colonnes financières")
    cols = [
        "chiffre_affaires", "resultat_net", "resultat_exercice",
        "capitaux_propres", "total_actif", "total_passif",
        "dettes_total", "dettes_fournisseurs", "dettes_financieres",
        "stocks_net", "creances_clients_net",
        "dotations_amortissements", "resultat_exploitation",
        "charges_personnel", "ebe", "effectif_moyen",
        "capacite_autofinancement", "bfr",
    ]
    union = " UNION ALL ".join([
        f"SELECT '{c}' as col, ROUND(100.0 * SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_null FROM raw_bilans"
        for c in cols
    ])
    nulls = con.execute(f"SELECT * FROM ({union}) ORDER BY pct_null DESC").df()
    print(nulls.to_string(index=False))

    # ── 2. DISTRIBUTION TYPE BILAN ───────────────────────────────
    section("2. DISTRIBUTION TYPE BILAN")
    print(con.execute("""
        SELECT type_bilan,
          COUNT(*) as nb,
          ROUND(MEDIAN(chiffre_affaires)/1000, 0) as ca_median_k,
          ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
        FROM raw_bilans WHERE chiffre_affaires > 0
        GROUP BY type_bilan ORDER BY nb DESC
    """).df().to_string(index=False))

    # ── 3. SEGMENTATION CA 2023 ──────────────────────────────────
    section("3. SEGMENTATION PAR CA — 2023")
    print(con.execute("""
        SELECT
          CASE
            WHEN chiffre_affaires <= 0        THEN '0. Nul/négatif'
            WHEN chiffre_affaires < 500000    THEN '1. Micro <500k'
            WHEN chiffre_affaires < 2000000   THEN '2. TPE 500k-2M'
            WHEN chiffre_affaires < 10000000  THEN '3. PME 2M-10M'
            WHEN chiffre_affaires < 50000000  THEN '4. ETI 10M-50M'
            WHEN chiffre_affaires < 250000000 THEN '5. Grande 50M-250M'
            ELSE                                   '6. >250M'
          END as segment,
          COUNT(*) as nb,
          ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
        FROM raw_bilans WHERE annee = 2023
        GROUP BY segment ORDER BY segment
    """).df().to_string(index=False))

    # ── 4. OUTLIERS ──────────────────────────────────────────────
    section("4. OUTLIERS")
    r = con.execute("""
        SELECT
          SUM(CASE WHEN chiffre_affaires IS NULL OR chiffre_affaires <= 0 THEN 1 ELSE 0 END) as ca_nul,
          SUM(CASE WHEN chiffre_affaires > 1000000000 THEN 1 ELSE 0 END) as ca_sup_1B,
          SUM(CASE WHEN capitaux_propres < 0 THEN 1 ELSE 0 END) as cp_negatifs,
          SUM(CASE WHEN resultat_net > chiffre_affaires AND chiffre_affaires > 0 THEN 1 ELSE 0 END) as rn_sup_ca
        FROM raw_bilans
    """).fetchone()
    print(f"  CA nul ou négatif     : {r[0]:,}")
    print(f"  CA > 1 milliard       : {r[1]:,}")
    print(f"  Capitaux propres < 0  : {r[2]:,}")
    print(f"  Résultat net > CA     : {r[3]:,}")

    # ── 5. DOUBLONS ──────────────────────────────────────────────
    section("5. DOUBLONS (siren, annee)")
    doublons = con.execute("""
        SELECT COUNT(*) as nb_doublons FROM (
          SELECT siren, annee FROM raw_bilans
          GROUP BY siren, annee HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    print(f"  Paires (siren, annee) en doublon : {doublons:,}")

    # ── 6. COHÉRENCE BILAN ───────────────────────────────────────
    section("6. COHÉRENCE BILAN (actif ≈ passif)")
    print(con.execute("""
        SELECT
          COUNT(*) as total_avec_actif_passif,
          SUM(CASE WHEN ABS(total_actif - total_passif) > total_actif * 0.05
              THEN 1 ELSE 0 END) as incoherents,
          ROUND(100.0 * SUM(CASE WHEN ABS(total_actif - total_passif) > total_actif * 0.05
              THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_incoherents
        FROM raw_bilans
        WHERE total_actif IS NOT NULL
          AND total_passif IS NOT NULL
          AND total_actif > 0
    """).df().to_string(index=False))

    con.close()
    print("\n[DONE] Exploration terminée — règles de nettoyage prêtes pour stg_bilans Day 04")

if __name__ == "__main__":
    print("=== FinScope Day 03 — Exploration & Nettoyage ===")
    run()