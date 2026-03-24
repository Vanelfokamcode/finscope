-- stg_entreprises.sql
-- Staging Sirene INSEE : SIREN + code_naf + secteur macro
-- Source : raw_entreprises (chargée par ingestion/download_rne.py)
-- Couverture : 100% des SIRENs de raw_bilans

WITH source AS (

    SELECT * FROM raw_entreprises

),

cleaned AS (

    SELECT
        siren,
        UPPER(TRIM(code_naf))                           AS code_naf,
        LEFT(UPPER(TRIM(code_naf)), 2)                  AS naf_division,

        CASE
            WHEN LEFT(code_naf, 2) BETWEEN '01' AND '03' THEN 'A — Agriculture'
            WHEN LEFT(code_naf, 2) BETWEEN '05' AND '09' THEN 'B — Industries extractives'
            WHEN LEFT(code_naf, 2) BETWEEN '10' AND '33' THEN 'C — Industrie manufacturière'
            WHEN LEFT(code_naf, 2) = '35'               THEN 'D — Énergie'
            WHEN LEFT(code_naf, 2) BETWEEN '36' AND '39' THEN 'E — Eau, déchets'
            WHEN LEFT(code_naf, 2) BETWEEN '41' AND '43' THEN 'F — Construction'
            WHEN LEFT(code_naf, 2) BETWEEN '45' AND '47' THEN 'G — Commerce'
            WHEN LEFT(code_naf, 2) BETWEEN '49' AND '53' THEN 'H — Transport'
            WHEN LEFT(code_naf, 2) BETWEEN '55' AND '56' THEN 'I — Hôtellerie/Restauration'
            WHEN LEFT(code_naf, 2) BETWEEN '58' AND '63' THEN 'J — Information/Communication'
            WHEN LEFT(code_naf, 2) BETWEEN '64' AND '66' THEN 'K — Finance/Assurance'
            WHEN LEFT(code_naf, 2) = '68'               THEN 'L — Immobilier'
            WHEN LEFT(code_naf, 2) BETWEEN '69' AND '75' THEN 'M — Services professionnels'
            WHEN LEFT(code_naf, 2) BETWEEN '77' AND '82' THEN 'N — Services admin'
            WHEN LEFT(code_naf, 2) = '84'               THEN 'O — Administration publique'
            WHEN LEFT(code_naf, 2) = '85'               THEN 'P — Enseignement'
            WHEN LEFT(code_naf, 2) BETWEEN '86' AND '88' THEN 'Q — Santé/Action sociale'
            ELSE 'Z — Autres'
        END                                             AS secteur_macro,

        COALESCE(denomination, '')                      AS denomination,
        COALESCE(categorie, '')                         AS categorie

    FROM source
    WHERE siren IS NOT NULL
      AND code_naf IS NOT NULL
      AND LENGTH(TRIM(code_naf)) >= 4

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY siren
        ORDER BY code_naf
    ) = 1

)

SELECT * FROM cleaned
