-- mart_benchmark_sectoriel.sql
-- Statistiques distributionnelles par secteur NAF × année
-- P25, P50 (médiane), P75 pour chaque indicateur financier clé
-- Grain : 1 ligne par naf_division × annee_fiscale
-- Source : mart_indicateurs × stg_entreprises
-- Consommé par : mart_score_sectoriel (Day 11), API /benchmark (Day 13)

WITH base AS (

    -- Joindre les indicateurs avec le NAF de l'entreprise
    SELECT
        i.*,
        e.code_naf,
        e.naf_division,
        e.secteur_macro

    FROM {{ ref('mart_indicateurs') }} i
    JOIN {{ ref('stg_entreprises') }} e
        USING (siren)

    -- Exclure les bilans sans CA (holdings purs, entreprises sans activité)
    WHERE i.chiffre_affaires > 0

),

benchmark AS (

    SELECT
        naf_division,
        secteur_macro,
        annee_fiscale,

        -- ── Volumétrie ───────────────────────────────────────────────
        COUNT(DISTINCT siren)                               AS nb_entreprises,

        -- ── CA — taille du secteur ───────────────────────────────────
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
            (ORDER BY chiffre_affaires) / 1000, 0)           AS p50_ca_k,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP
            (ORDER BY chiffre_affaires) / 1000, 0)           AS p25_ca_k,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP
            (ORDER BY chiffre_affaires) / 1000, 0)           AS p75_ca_k,

        -- ── Marge nette ──────────────────────────────────────────────
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
            (ORDER BY marge_nette_pct), 2)                    AS p50_marge_nette,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP
            (ORDER BY marge_nette_pct), 2)                    AS p25_marge_nette,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP
            (ORDER BY marge_nette_pct), 2)                    AS p75_marge_nette,

        -- ── Marge EBITDA ─────────────────────────────────────────────
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
            (ORDER BY marge_ebitda_pct), 2)                   AS p50_marge_ebitda,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP
            (ORDER BY marge_ebitda_pct), 2)                   AS p25_marge_ebitda,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP
            (ORDER BY marge_ebitda_pct), 2)                   AS p75_marge_ebitda,

        -- ── BFR en jours ─────────────────────────────────────────────
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
            (ORDER BY bfr_jours), 1)                           AS p50_bfr_jours,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP
            (ORDER BY bfr_jours), 1)                           AS p25_bfr_jours,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP
            (ORDER BY bfr_jours), 1)                           AS p75_bfr_jours,

        -- ── Ratio d'endettement ──────────────────────────────────────
        -- Exclure les CP négatifs (ratio non interprétable)
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
            (ORDER BY ratio_endettement), 3)                   AS p50_endettement,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP
            (ORDER BY ratio_endettement), 3)                   AS p25_endettement,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP
            (ORDER BY ratio_endettement), 3)                   AS p75_endettement,

        -- ── Ratio de liquidité ───────────────────────────────────────
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
            (ORDER BY ratio_liquidite), 3)                     AS p50_liquidite,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP
            (ORDER BY ratio_liquidite), 3)                     AS p25_liquidite,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP
            (ORDER BY ratio_liquidite), 3)                     AS p75_liquidite,

        -- ── % entreprises en CP négatifs (signal risque sectoriel) ──
        ROUND(100.0 * COUNT(*) FILTER (
            WHERE capitaux_propres_negatifs = TRUE
        ) / COUNT(*), 1)                                      AS pct_cp_negatifs

    FROM base

    -- Filtrer les secteurs trop petits pour un benchmark fiable
    GROUP BY naf_division, secteur_macro, annee_fiscale
    HAVING COUNT(DISTINCT siren) >= 50

)

SELECT * FROM benchmark
ORDER BY annee_fiscale DESC, nb_entreprises DESC