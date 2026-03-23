"""
FinScope API — Day 07
FastAPI + DuckDB : expose les marts dbt en REST JSON
Endpoints : /company · /indicateurs · /evolution
Port : 8003
"""

import math
from pathlib import Path
from typing import Optional, List

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ── Connexion DuckDB ──────────────────────────────────────────
DB_PATH = Path(__file__).parent.parent / "data" / "finscope.duckdb"
con = duckdb.connect(str(DB_PATH), read_only=True)

# ── App FastAPI ───────────────────────────────────────────────
app = FastAPI(
    title="FinScope API",
    description="Intelligence financière sur données INPI — entreprises françaises",
    version="0.1.0",
)

# CORS — autorise le frontend HTML Day 08 à appeler l'API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Utilitaire NaN → None ─────────────────────────────────────
def clean_nan(obj):
    """Remplace NaN/Inf par None pour la sérialisation JSON."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(i) for i in obj]
    return obj


# ── Endpoint racine ───────────────────────────────────────────
@app.get("/")
def root():
    return {
        "api": "FinScope",
        "version": "0.1.0",
        "endpoints": ["/company", "/indicateurs", "/evolution", "/docs"],
        "data": "INPI bulk · data.gouv.fr · entreprises FR",
    }


# ── GET /company ──────────────────────────────────────────────
# Informations de base sur une entreprise (stg_bilans)
# Paramètre : siren (obligatoire)
@app.get("/company")
def get_company(
    siren: str = Query(..., description="SIREN 9 chiffres", min_length=9, max_length=9)
):
    """
    Retourne les bilans disponibles pour un SIREN donné.
    Liste les années disponibles, types de bilans, dates de clôture.
    """
    rows = con.execute("""
        SELECT
            siren,
            annee_fiscale,
            date_cloture,
            type_bilan,
            chiffre_affaires,
            capitaux_propres,
            total_actif
        FROM main.stg_bilans
        WHERE siren = ?
        ORDER BY annee_fiscale DESC
    """, [siren]).df().to_dict(orient="records")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"SIREN {siren} introuvable dans la base FinScope"
        )

    return {
        "siren": siren,
        "nb_exercices": len(rows),
        "annees": [r["annee_fiscale"] for r in rows],
        "bilans": clean_nan(rows),
    }


# ── GET /indicateurs ──────────────────────────────────────────
# Indicateurs financiers calculés (mart_indicateurs)
# Paramètres : siren (obligatoire), annee (optionnel)
@app.get("/indicateurs")
def get_indicateurs(
    siren: str = Query(..., description="SIREN 9 chiffres", min_length=9, max_length=9),
    annee: Optional[int] = Query(None, description="Année fiscale (ex: 2023). Si absent : toutes les années.")
):
    """
    Retourne les indicateurs financiers calculés pour un SIREN.
    EBITDA, BFR, marge nette, ratios de liquidité et d'endettement.
    Optionnel : filtrer par année.
    """
    query = """
        SELECT
            siren, annee_fiscale, date_cloture, type_bilan,
            chiffre_affaires,
            ebitda, marge_ebitda_pct,
            resultat_net, marge_nette_pct,
            bfr, bfr_jours,
            ratio_liquidite, flag_liquidite,
            ratio_endettement, flag_endettement,
            capitaux_propres, total_actif, dettes_total,
            effectif_moyen, capitaux_propres_negatifs
        FROM main.mart_indicateurs
        WHERE siren = ?
    """
    params = [siren]

    if annee:
        query += " AND annee_fiscale = ?"
        params.append(annee)

    query += " ORDER BY annee_fiscale DESC"

    rows = con.execute(query, params).df().to_dict(orient="records")

    if not rows:
        msg = f"SIREN {siren} introuvable"
        if annee:
            msg += f" pour l'année {annee}"
        raise HTTPException(status_code=404, detail=msg)

    return {
        "siren": siren,
        "nb_exercices": len(rows),
        "indicateurs": clean_nan(rows),
    }


# ── GET /evolution ────────────────────────────────────────────
# Évolution temporelle N vs N-1 (mart_evolution)
# Paramètres : siren (obligatoire)
@app.get("/evolution")
def get_evolution(
    siren: str = Query(..., description="SIREN 9 chiffres", min_length=9, max_length=9)
):
    """
    Retourne l'évolution temporelle d'une entreprise.
    Variation CA N-1, delta marge, delta BFR, flag_degradation.
    """
    rows = con.execute("""
        SELECT
            siren, annee_fiscale,
            chiffre_affaires, ca_n1, variation_ca_pct,
            marge_nette_pct, marge_nette_n1, delta_marge_nette,
            bfr_jours, bfr_jours_n1, delta_bfr_jours,
            ebitda, variation_ebitda_pct,
            ratio_endettement, flag_endettement,
            flag_liquidite, flag_degradation,
            baisse_ca, baisse_marge
        FROM main.mart_evolution
        WHERE siren = ?
        ORDER BY annee_fiscale DESC
    """, [siren]).df().to_dict(orient="records")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"SIREN {siren} introuvable"
        )

    # Calculer un résumé du trend
    alertes = [r for r in rows if r.get("flag_degradation") == "ALERTE"]
    derniere = rows[0]  # année la plus récente (ORDER BY DESC)

    return {
        "siren": siren,
        "nb_exercices": len(rows),
        "derniere_annee": derniere["annee_fiscale"],
        "flag_degradation_actuel": derniere["flag_degradation"],
        "nb_annees_alerte": len(alertes),
        "evolution": clean_nan(rows),
    }


# ── GET /alertes ──────────────────────────────────────────────
# Liste des entreprises en ALERTE pour une année donnée
# Bonus Day 07 — utile pour Phase 5
@app.get("/alertes")
def get_alertes(
    annee: int = Query(2023, description="Année fiscale"),
    min_ca: int = Query(500000, description="CA minimum en euros"),
    limit: int = Query(50, le=500, description="Nombre max de résultats")
):
    """
    Retourne les entreprises en flag ALERTE pour une année.
    CA minimum configurable. Limite configurable (max 500).
    """
    rows = con.execute("""
        SELECT
            siren,
            chiffre_affaires,
            variation_ca_pct,
            delta_marge_nette,
            flag_endettement,
            flag_liquidite
        FROM main.mart_evolution
        WHERE annee_fiscale = ?
          AND flag_degradation = 'ALERTE'
          AND chiffre_affaires >= ?
        ORDER BY variation_ca_pct ASC
        LIMIT ?
    """, [annee, min_ca, limit]).df().to_dict(orient="records")

    return {
        "annee": annee,
        "min_ca": min_ca,
        "nb_alertes": len(rows),
        "alertes": clean_nan(rows),
    }