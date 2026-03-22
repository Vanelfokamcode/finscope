import os, requests, duckdb, hashlib
from pathlib import Path
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────
RAW_DIR  = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# URL stable — pointe toujours vers la dernière version
SF_URL    = "https://www.data.gouv.fr/api/1/datasets/r/c4ac8f98-2c97-4417-9070-0cbb9de03875"
SF_LOCAL  = RAW_DIR / "export-detail-bilan.parquet"
SF_SHA1   = "9f904830150d68bec8ce5a1afca13813d3b0920e"  # version fév. 2026
DB_PATH   = "data/finscope.duckdb"


def sha1_file(path: Path) -> str:
    """Calcule le SHA1 d'un fichier pour vérifier l'intégrité."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def download_parquet() -> Path:
    """
    Télécharge le parquet Signaux Faibles si absent ou corrompu.
    Vérifie le SHA1 après téléchargement.
    """
    if SF_LOCAL.exists():
        print(f"[CACHE] {SF_LOCAL} existe déjà.")
        return SF_LOCAL

    print(f"[DOWNLOAD] Signaux Faibles parquet (2.6 Go)...")
    print(f"[URL] {SF_URL}")

    resp = requests.get(SF_URL, stream=True, timeout=120)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    with open(SF_LOCAL, "wb") as f, tqdm(
        total=total, unit="B", unit_scale=True, desc="export-detail-bilan.parquet"
    ) as bar:
        for chunk in resp.iter_content(65536):
            f.write(chunk)
            bar.update(len(chunk))

    # Vérification intégrité
    actual_sha1 = sha1_file(SF_LOCAL)
    if actual_sha1 != SF_SHA1:
        print(f"[WARN] SHA1 différent — fichier mis à jour sur data.gouv.fr")
        print(f"  Attendu : {SF_SHA1}")
        print(f"  Obtenu  : {actual_sha1}")
        print(f"  → Mettre à jour SF_SHA1 dans ce script")
    else:
        print(f"[OK] SHA1 vérifié ✓")

    return SF_LOCAL


def explore_parquet(parquet_path: Path) -> None:
    """Explore le parquet avec DuckDB et affiche les stats de base."""
    print(f"\n[EXPLORE] {parquet_path}")
    con = duckdb.connect()
    p = str(parquet_path)

    # Nombre de lignes
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{p}')").fetchone()[0]
    print(f"  Lignes totales    : {n:,}")

    # SIREN uniques
    ns = con.execute(f"SELECT COUNT(DISTINCT siren) FROM read_parquet('{p}')").fetchone()[0]
    print(f"  SIREN uniques     : {ns:,}")

    # Colonnes + types
    schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").df()
    print(f"\n  Colonnes ({len(schema)}) :")
    for _, row in schema.iterrows():
        print(f"    {row['column_name']:<35} {row['column_type']}")

    # Années disponibles
    try:
        years = con.execute(
            f"SELECT annee, COUNT(*) n FROM read_parquet('{p}') GROUP BY annee ORDER BY annee"
        ).df()
        print(f"\n  Années disponibles :\n{years.to_string(index=False)}")
    except Exception as e:
        print(f"  [WARN] colonne 'annee' introuvable : {e}")

    con.close()


# ── MAIN ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== FinScope Day 01 — Signaux Faibles parquet ===")
    parquet = download_parquet()
    explore_parquet(parquet)
    print("\n[DONE] Day 01 terminé — prêt pour Day 02 (load DuckDB)")