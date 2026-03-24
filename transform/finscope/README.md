## FinScope — Intelligence financière sur données INPI

**Stack** : Python · DuckDB · dbt · FastAPI · HTML vanilla  
**Source** : INPI bulk · data.gouv.fr · 4.8M bilans français  
**API** : http://localhost:8003/docs

### Phase 1 — FP&A Core — Découvertes

- **9.4%** des entreprises françaises ont des capitaux propres négatifs (456k entreprises)  
- **CA médian PME** : +51% entre 2016 et 2024 (638k → 964k€)  
- **COVID 2020** directement visible : chute du CA médian de 10% et pic d'alertes  
- **~15%** des entreprises avec CA > 500k€ sont en flag ALERTE en 2023  
- **40% des bilans** type "Très grande entreprise" sont des artefacts d'overflow INT32  

### Lancer en local

```bash
# 1. Télécharger le parquet INPI (~2.6 Go)
python ingestion/download_sf.py

# 2. Charger dans DuckDB
python ingestion/load_to_duckdb.py

# 3. Transformer avec dbt
cd transform/finscope && dbt run && dbt test

# 4. Lancer l'API
uvicorn api.main:app --port 8003

# 5. Ouvrir le dashboard
open frontend/index.html
```