# Onboarding Guide — New Data Engineer

## Prerequisites

- Python 3.11+
- Snowflake account with `DATA_ENGINEER` role
- Git + GitHub access to this repository
- VS Code or equivalent IDE with dbt Power User extension

---

## Day 1: Environment Setup

```bash
# 1. Clone repository
git clone https://github.com/hsg09/data_vault_2_0.git
cd data_vault_2_0

# 2. Create virtual environment
python -m venv .venv && source .venv/bin/activate

# 3. Install dependencies
pip install -e ".[dev]"

# 4. Configure environment
cp .env.example .env
# Fill in: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD

# 5. Install dbt packages
dbt deps --profiles-dir .

# 6. Verify connection
dbt debug --profiles-dir .

# 7. Build documentation
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```

---

## Day 2: Understanding the Architecture

1. Read [Architecture Guide](architecture.md) — understand Bronze/Silver/Gold layers
2. Read [Data Vault 2.0 Guide](data_vault_guide.md) — understand Hub/Link/Satellite patterns
3. Explore the dbt lineage graph: `dbt docs serve --profiles-dir .`
4. Run the full pipeline in dev: `dbt run --profiles-dir . --target dev`

---

## Development Workflow

### Adding a New Entity

1. **Create staging model**: `models/bronze/staging/stg_{source}__{entity}.sql`
   - Add hash keys (`HK_{ENTITY}`), hash diffs, metadata columns
2. **Create Hub**: `models/bronze/hubs/hub_{entity}.sql`
3. **Create Satellite(s)**: `models/bronze/satellites/sat_{entity}_{descriptor}.sql`
4. **Create Link** (if relationship): `models/bronze/links/link_{entity1}_{entity2}.sql`
5. **Add schema tests**: Update corresponding `_*.yml` files
6. **Run locally**: `dbt run --select +hub_{entity}+ --profiles-dir .`
7. **Test**: `dbt test --select +hub_{entity}+ --profiles-dir .`
8. **Open PR**: CI will build only your modified models

### Key dbt Commands

```bash
# Run specific model + downstream
dbt run --select +model_name+ --profiles-dir .

# Run by tag
dbt run --select tag:bronze --profiles-dir .

# Test specific model
dbt test --select model_name --profiles-dir .

# Compile SQL without executing
dbt compile --select model_name --profiles-dir .

# Full refresh (rebuild incremental from scratch)
dbt run --full-refresh --select model_name --profiles-dir .
```

---

## Key Macros

| Macro | Usage | Purpose |
|---|---|---|
| `hash_key(['COL1', 'COL2'])` | Staging models | Generate SHA-256 hash key |
| `hash_diff(['COL1', 'COL2'])` | Staging models | Generate SHA-256 hash diff |
| `audit_columns()` | Any model | Add `_DW_LOADED_AT`, `_DW_MODEL_NAME` |
| `safe_cast('COL', 'TYPE')` | Any model | TRY_CAST with fallback |
| `incremental_lookback()` | Incremental models | Standard lookback filter |

---

## Contacts

| Role | Responsibility |
|---|---|
| Platform Admin | Infrastructure, Terraform, Snowflake account management |
| Data Steward | Governance, tagging, masking policy management |
| Data Engineer | Pipeline development, dbt models, testing |
