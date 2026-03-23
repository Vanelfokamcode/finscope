WITH source AS (
    SELECT * FROM raw_bilans
),

cleaned AS (
    SELECT
        -- Identifiants
        siren,
        date_cloture_exercice                                        AS date_cloture,
        CAST(annee AS INTEGER)                                       AS annee_fiscale,
        type_bilan,

        -- Compte de résultat (CAST BIGINT pour éviter overflow INT32)
        CAST(COALESCE(chiffre_affaires, 0)        AS BIGINT)        AS chiffre_affaires,
        CAST(COALESCE(resultat_net, 0)             AS BIGINT)        AS resultat_net,
        CAST(COALESCE(valeur_ajoutee, 0)           AS BIGINT)        AS valeur_ajoutee,
        CAST(COALESCE(ebe, 0)                      AS BIGINT)        AS ebe,
        CAST(COALESCE(charges_personnel, 0)        AS BIGINT)        AS charges_personnel,
        CAST(COALESCE(dotations_amortissements, 0) AS BIGINT)        AS dotations_amortissements,

        -- Bilan actif
        CAST(COALESCE(total_actif, 0)              AS BIGINT)        AS total_actif,
        CAST(COALESCE(immob_corpo_net, 0)          AS BIGINT)        AS immob_corpo_net,
        CAST(COALESCE(stocks_net, 0)               AS BIGINT)        AS stocks_net,
        CAST(COALESCE(creances_clients_net, 0)     AS BIGINT)        AS creances_clients_net,
        CAST(COALESCE(disponibilites_net, 0)       AS BIGINT)        AS disponibilites_net,

        -- Bilan passif
        CAST(COALESCE(capitaux_propres, 0)         AS BIGINT)        AS capitaux_propres,
        CAST(COALESCE(capital_social, 0)           AS BIGINT)        AS capital_social,
        CAST(COALESCE(reserves, 0)                 AS BIGINT)        AS reserves,
        CAST(COALESCE(resultat_exercice, 0)        AS BIGINT)        AS resultat_exercice,
        CAST(COALESCE(dettes_total, 0)             AS BIGINT)        AS dettes_total,
        CAST(COALESCE(dettes_financieres, 0)       AS BIGINT)        AS dettes_financieres,
        CAST(COALESCE(dettes_fournisseurs, 0)      AS BIGINT)        AS dettes_fournisseurs,
        CAST(COALESCE(dettes_fiscales, 0)          AS BIGINT)        AS dettes_fiscales,
        CAST(COALESCE(actif_circulant_net, 0) AS BIGINT) AS actif_circulant_net,

        -- Effectifs
        COALESCE(effectif_moyen, 0)                                  AS effectif_moyen,

        -- Colonnes calculées
        COALESCE(capitaux_propres, 0) < 0                            AS capitaux_propres_negatifs,

        -- Clé surrogate
        siren || '_' || CAST(annee AS VARCHAR)                       AS bilan_key

    FROM source

    WHERE
        siren IS NOT NULL
        AND annee BETWEEN 2010 AND 2025
        AND confidentiality = 'Public'

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY siren, annee
        ORDER BY CASE type_bilan WHEN 'C' THEN 1 WHEN 'K' THEN 2 ELSE 3 END
    ) = 1
)

SELECT * FROM cleaned
