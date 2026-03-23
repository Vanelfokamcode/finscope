import duckdb
con = duckdb.connect("data/finscope.duckdb")

print("=== DISTRIBUTION FLAG DÉGRADATION 2023 ===")
print(con.execute("""
    SELECT
        flag_degradation,
        COUNT(*) as nb,
        ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 1) as pct
    FROM main.mart_evolution
    WHERE annee_fiscale = 2023
    GROUP BY flag_degradation
    ORDER BY nb DESC
""").df().to_string(index=False))
con.close()