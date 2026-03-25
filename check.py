import duckdb
con = duckdb.connect("data/finscope.duckdb")

print("=== BENCHMARK MARGE NETTE 2023 (top secteurs) ===")
print(con.execute("""
    SELECT
        naf_division,
        secteur_macro,
        nb_entreprises,
        p25_marge_nette,
        p50_marge_nette,
        p75_marge_nette,
        pct_cp_negatifs
    FROM main.mart_benchmark_sectoriel
    WHERE annee_fiscale = 2023
      AND nb_entreprises >= 200
    ORDER BY p50_marge_nette DESC
    LIMIT 15
""").df().to_string(index=False))

print("\n=== BENCHMARK BFR 2023 (qui a le plus de trésorerie immobilisée) ===")
print(con.execute("""
    SELECT
        naf_division,
        secteur_macro,
        nb_entreprises,
        p25_bfr_jours,
        p50_bfr_jours,
        p75_bfr_jours
    FROM main.mart_benchmark_sectoriel
    WHERE annee_fiscale = 2023
      AND nb_entreprises >= 200
    ORDER BY p50_bfr_jours DESC
    LIMIT 10
""").df().to_string(index=False))

print("\n=== SITUER UNE ENTREPRISE TECH (naf=62) ===")
print(con.execute("""
    SELECT annee_fiscale,
           nb_entreprises,
           p25_marge_nette, p50_marge_nette, p75_marge_nette,
           p50_bfr_jours,
           p50_endettement,
           pct_cp_negatifs
    FROM main.mart_benchmark_sectoriel
    WHERE naf_division = '62'
    ORDER BY annee_fiscale DESC
    LIMIT 5
""").df().to_string(index=False))
con.close()