-- mart_indicateurs.sql
-- Calcul des indicateurs financiers clés à partir de stg_bilans
-- EBITDA · BFR · liquidité générale · endettement · marge nette
-- Source : stg_bilans (Day 04)
-- Consommé par : mart_evolution (Day 06), mart_zscore (Day 18)

WITH base AS (

    -- Référence via dbt ref() — dbt résout le schéma automatiquement
    SELECT * FROM {{ ref('stg_bilans') }}

),

indicateurs AS (

    SELECT
        -- ── Identifiants ──────────────────────────────────────────────
        bilan_key,
        siren,
        annee_fiscale,
        date_cloture,
        type_bilan,
        capitaux_propres_negatifs,

        -- ── Données brutes clés (pour les marts suivants) ─────────────
        chiffre_affaires,
        resultat_net,
        capitaux_propres,
        total_actif,
        dettes_total,
        dettes_financieres,
        effectif_moyen,

        -- ── 1. EBITDA ─────────────────────────────────────────────────
        -- EBE (PCG GV) + Dotations amortissements (PCG FQ)
        -- Les deux sont COALESCE(x,0) dans stg_bilans
        ebe + dotations_amortissements                      AS ebitda,

        CASE
            WHEN chiffre_affaires > 0
            THEN ROUND(
                100.0 * (ebe + dotations_amortissements)
                / NULLIF(chiffre_affaires, 0), 2
            )
            ELSE NULL
        END                                                AS marge_ebitda_pct,

        -- ── 2. Marge nette ────────────────────────────────────────────
        CASE
            WHEN chiffre_affaires > 0
            THEN ROUND(
                100.0 * CAST(resultat_net AS DECIMAL)
                / NULLIF(chiffre_affaires, 0), 2
            )
            ELSE NULL
        END                                                AS marge_nette_pct,

        -- ── 3. BFR ────────────────────────────────────────────────────
        stocks_net + creances_clients_net - dettes_fournisseurs
                                                            AS bfr,

        CASE
            WHEN chiffre_affaires > 0
            THEN ROUND(
                (stocks_net + creances_clients_net - dettes_fournisseurs)
                * 365.0 / NULLIF(chiffre_affaires, 0), 1
            )
            ELSE NULL
        END                                                AS bfr_jours,

        -- ── 4. Ratio de liquidité générale ───────────────────────────
        -- Actif circulant / Dettes CT
        -- Dettes CT ≈ dettes_total − dettes_financières (LT)
        ROUND(
            CAST(actif_circulant_net AS DECIMAL)
            / NULLIF(
                GREATEST(dettes_total - dettes_financieres, 0),
                0
            ), 3
        )                                                   AS ratio_liquidite,

        CASE
            WHEN CAST(actif_circulant_net AS DECIMAL)
                 / NULLIF(GREATEST(dettes_total - dettes_financieres, 0), 0)
                 >= 1.0  THEN 'LIQUIDE'
            WHEN CAST(actif_circulant_net AS DECIMAL)
                 / NULLIF(GREATEST(dettes_total - dettes_financieres, 0), 0)
                 >= 0.8  THEN 'TENSION'
            ELSE 'RISQUE'
        END                                                AS flag_liquidite,

        -- ── 5. Ratio d'endettement ───────────────────────────────────
        -- Dettes financières / Capitaux propres
        -- NULL si CP ≤ 0 (non interprétable)
        CASE
            WHEN capitaux_propres <= 0 THEN NULL
            ELSE ROUND(
                CAST(dettes_financieres AS DECIMAL)
                / NULLIF(capitaux_propres, 0), 3
            )
        END                                                AS ratio_endettement,

        CASE
            WHEN capitaux_propres <= 0          THEN 'CP_NEGATIFS'
            WHEN dettes_financieres = 0         THEN 'SANS_DETTE'
            WHEN CAST(dettes_financieres AS DECIMAL)
                 / NULLIF(capitaux_propres,0) > 2  THEN 'TRES_ENDETTÉ'
            WHEN CAST(dettes_financieres AS DECIMAL)
                 / NULLIF(capitaux_propres,0) > 1  THEN 'ENDETTÉ'
            ELSE 'SAIN'
        END                                                AS flag_endettement

    FROM base

)

SELECT * FROM indicateurs