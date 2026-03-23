import duckdb
con = duckdb.connect("data/finscope.duckdb")

print("=== DISTRIBUTION FLAG ENDETTEMENT 2023 ===")
print(con.execute("""
    SELECT flag_endettement,
           COUNT(*) as nb,
           ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 1) as pct
    FROM  main.mart_indicateurs
    WHERE annee_fiscale = 2023
    GROUP BY flag_endettement
    ORDER BY nb DESC
""").df().to_string(index=False))

print("\n=== MARGE EBITDA MÉDIANE PAR TAILLE 2023 ===")
print(con.execute("""
    SELECT
        CASE
            WHEN chiffre_affaires < 500000    THEN '1.Micro'
            WHEN chiffre_affaires < 2000000   THEN '2.TPE'
            WHEN chiffre_affaires < 10000000  THEN '3.PME'
            WHEN chiffre_affaires < 50000000  THEN '4.ETI'
            ELSE '5.Grande'
        END as segment,
        COUNT(*) as nb,
        ROUND(MEDIAN(marge_ebitda_pct), 1) as ebitda_median_pct,
        ROUND(MEDIAN(marge_nette_pct), 1)  as marge_nette_median_pct,
        ROUND(MEDIAN(bfr_jours), 0)        as bfr_jours_median
    FROM main.mart_indicateurs
    WHERE annee_fiscale = 2023
      AND chiffre_affaires > 0
      AND marge_ebitda_pct IS NOT NULL
    GROUP BY segment
    ORDER BY segment
""").df().to_string(index=False))

con.close()