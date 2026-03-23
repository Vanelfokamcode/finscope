-- mart_evolution.sql
-- Vue temporelle : variation N vs N-1 pour chaque entreprise
-- Indicateurs : CA, marge nette, BFR jours, EBITDA, ratio endettement
-- Signal forensic : flag_degradation (dégradation continue 2 ans)
-- Source : mart_indicateurs (Day 05)
-- Consommé par : mart_signaux_risque (Day 19), mart_zscore (Day 18)

WITH base AS (

    SELECT * FROM {{ ref('mart_indicateurs') }}

),

-- ── Étape 1 : calculer les valeurs N-1 avec LAG() ────────────
avec_lag AS (

    SELECT
        bilan_key,
        siren,
        annee_fiscale,
        date_cloture,
        type_bilan,

        -- Valeurs de l'année courante
        chiffre_affaires,
        marge_nette_pct,
        marge_ebitda_pct,
        ebitda,
        bfr,
        bfr_jours,
        ratio_endettement,
        ratio_liquidite,
        flag_endettement,
        flag_liquidite,
        capitaux_propres_negatifs,
        effectif_moyen,

        -- Valeurs N-1 (année précédente du même SIREN)
        LAG(chiffre_affaires,   1) OVER (w)  AS ca_n1,
        LAG(marge_nette_pct,    1) OVER (w)  AS marge_nette_n1,
        LAG(marge_ebitda_pct,   1) OVER (w)  AS marge_ebitda_n1,
        LAG(ebitda,             1) OVER (w)  AS ebitda_n1,
        LAG(bfr_jours,          1) OVER (w)  AS bfr_jours_n1,
        LAG(ratio_endettement,  1) OVER (w)  AS ratio_endettement_n1,
        LAG(effectif_moyen,     1) OVER (w)  AS effectif_n1

    FROM base

    -- Nommer la window pour éviter la répétition
    WINDOW w AS (
        PARTITION BY siren
        ORDER BY annee_fiscale
    )

),

-- ── Étape 2 : calculer les variations ────────────────────────
avec_variations AS (

    SELECT
        *,

        -- Variation CA en % = (CA_N - CA_N1) / CA_N1 × 100
        CASE
            WHEN ca_n1 IS NULL OR ca_n1 = 0 THEN NULL
            ELSE ROUND(
                100.0 * (CAST(chiffre_affaires AS DECIMAL) - ca_n1)
                / NULLIF(ca_n1, 0), 2
            )
        END AS variation_ca_pct,

        -- Variation marge nette (points de %) = marge_N - marge_N1
        CASE
            WHEN marge_nette_n1 IS NULL THEN NULL
            ELSE ROUND(marge_nette_pct - marge_nette_n1, 2)
        END AS delta_marge_nette,

        -- Variation BFR jours (jours) = BFR_N - BFR_N1
        CASE
            WHEN bfr_jours_n1 IS NULL THEN NULL
            ELSE ROUND(bfr_jours - bfr_jours_n1, 1)
        END AS delta_bfr_jours,

        -- Variation EBITDA en %
        CASE
            WHEN ebitda_n1 IS NULL OR ebitda_n1 = 0 THEN NULL
            ELSE ROUND(
                100.0 * (CAST(ebitda AS DECIMAL) - ebitda_n1)
                / NULLIF(ebitda_n1, 0), 2
            )
        END AS variation_ebitda_pct,

        -- Booleans de baisse pour cette année
        chiffre_affaires < ca_n1                             AS baisse_ca,
        marge_nette_pct  < marge_nette_n1                   AS baisse_marge

    FROM avec_lag

),

-- ── Étape 3 : flag dégradation sur 2 ans consécutifs ─────────
avec_flag AS (

    SELECT
        *,

        -- baisse_ca de l'année N-1 (regarder en arrière sur la colonne booléenne)
        LAG(baisse_ca,    1) OVER (
            PARTITION BY siren ORDER BY annee_fiscale
        ) AS baisse_ca_n1,

        LAG(baisse_marge, 1) OVER (
            PARTITION BY siren ORDER BY annee_fiscale
        ) AS baisse_marge_n1

    FROM avec_variations

),

-- ── Étape 4 : assembler le flag final ─────────────────────────
final AS (

    SELECT
        bilan_key,
        siren,
        annee_fiscale,
        date_cloture,
        type_bilan,

        -- Indicateurs courants
        chiffre_affaires,
        ca_n1,
        marge_nette_pct,
        marge_nette_n1,
        marge_ebitda_pct,
        ebitda,
        bfr_jours,
        bfr_jours_n1,
        ratio_endettement,
        ratio_liquidite,
        flag_endettement,
        flag_liquidite,
        capitaux_propres_negatifs,
        effectif_moyen,
        effectif_n1,

        -- Variations calculées
        variation_ca_pct,
        delta_marge_nette,
        delta_bfr_jours,
        variation_ebitda_pct,

        -- Booleans individuels
        baisse_ca,
        baisse_marge,

        -- ── FLAG DÉGRADATION ─────────────────────────────────────────
        -- ALERTE  : baisse CA ET baisse marge cette année ET l'année dernière
        -- VIGILANCE : baisse CA OU baisse marge (une seule année)
        -- STABLE  : pas de signal négatif
        -- NULL    : première année connue, pas de comparaison possible
        CASE
            WHEN baisse_ca IS NULL OR baisse_marge IS NULL
                THEN NULL
            WHEN baisse_ca = TRUE
                 AND baisse_marge = TRUE
                 AND baisse_ca_n1 = TRUE
                 AND baisse_marge_n1 = TRUE
                THEN 'ALERTE'
            WHEN baisse_ca = TRUE OR baisse_marge = TRUE
                THEN 'VIGILANCE'
            ELSE 'STABLE'
        END AS flag_degradation

    FROM avec_flag

)

SELECT * FROM final