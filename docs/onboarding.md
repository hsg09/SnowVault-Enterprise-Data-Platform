# Onboarding Guide — New Data Engineer

## Welcome to the Platform

Welcome to the Data Vault 2.0 Enterprise platform. This guide will walk you through setting up your local environment, understanding the architecture, and seamlessly executing your first model deployment.

---

## Prerequisites

Ensure you have the following provisioned before beginning:
- **Python 3.11+** installed locally.
- **Snowflake Account** with the `DATA_ENGINEER` role provisioned.
- **Git** authenticated with GitHub access to this repository.
- **IDE Access** (VS Code recommended, augmented with the `dbt Power User` extension).

---

## Day 1: Local Environment Setup

Execute the following commands sequentially to bootstrap your local environment.

```bash
# 1. Clone the repository
git clone https://github.com/hsg09/data_vault_2_0.git
cd data_vault_2_0

# 2. Create and activate a pristine virtual environment
python -m venv .venv
source .venv/bin/activate

# 3. Install platform dependencies (including dev constraints)
pip install -e ".[dev]"

# 4. Bootstrap environment variables
cp .env.example .env
```

> [!IMPORTANT]
> **Secure Credentials Required**
> You must securely inject your local `.env` file with `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and `SNOWFLAKE_PASSWORD`. **DO NOT** commit `.env` to version control. If you have not been securely transmitted these keys via enterprise secret management, please contact your Platform Admin immediately.

```bash
# 5. Download dbt packages/macros
dbt deps --profiles-dir .

# 6. Verify Snowflake connection
dbt debug --profiles-dir .

# 7. Generate and serve local documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

---

## Day 2: Understanding the Architecture

Before writing code, it is imperative to understand the structural layout of our environment:

1. Read the **[Architecture Guide](architecture.md)** — internalize the Bronze/Silver/Gold layer mechanics.
2. Read the **[Data Vault 2.0 Guide](data_vault_guide.md)** — understand the Hub, Link, and Satellite templating patterns.
3. Explore the live **dbt lineage graph** via your local `dbt docs serve`.
4. Validate your setup by running the full pipeline locally constraint to the dev environment:
   ```bash
   dbt run --profiles-dir . --target dev
   ```

---

## Development Workflow

### Adding a New Data Vault Entity

Follow this structured workflow to safely introduce a new business entity to the system:

1. **Create Staging Model**: Configure `models/bronze/staging/stg_{source}__{entity}.sql`. Be sure to implement hash keys (`HK_{ENTITY}`), hash diffs, and audit metadata columns.
2. **Create Hub**: Establish `models/bronze/hubs/hub_{entity}.sql`.
3. **Create Satellite(s)**: Implement `models/bronze/satellites/sat_{entity}_{descriptor}.sql`. Remember to split high-velocity and low-velocity attributes into separate satellites.
4. **Create Link** (Optional): If the entity possesses relationships, formulate `models/bronze/links/link_{entity1}_{entity2}.sql`.
5. **Add Schema Tests**: Ensure data quality by updating the sibling `_*.yml` configuration schemas.
6. **Execute Locally**: Build your specific namespace.
   ```bash
   dbt run --select +hub_{entity}+ --profiles-dir .
   ```
7. **Test Locally**: Assert your configurations.
   ```bash
   dbt test --select +hub_{entity}+ --profiles-dir .
   ```
8. **Open PR**: Push your branch and open a Pull Request. GitHub Actions will initialize an ephemeral Slim CI build focusing exclusively on your modifications.

> [!TIP]
> **Key dbt Commands for Daily Use**
> ```bash
> # Run a specific model and all its downstream dependents
> dbt run --select +model_name+ --profiles-dir .
> 
> # Execute all models tagged 'bronze'
> dbt run --select tag:bronze --profiles-dir .
> 
> # Compile sql locally without officially executing to the warehouse
> dbt compile --select model_name --profiles-dir .
> 
> # Force a full rebuild from scratch on an incremental model
> dbt run --full-refresh --select model_name --profiles-dir .
> ```

---

## Key dbt Macros

Familiarize yourself with these core macros extensively used across the platform models:

| Macro invocation | Implementation Layer | Functional Purpose |
|---|---|---|
| `hash_key(['COL1', 'COL2'])` | Staging Models | Generates a deterministic SHA-256 business key constraint |
| `hash_diff(['COL1', 'COL2'])` | Staging Models | Generates a SHA-256 state string for SCD Type 2 detection |
| `audit_columns()` | All Models | Automatically injects standard `_DW_LOADED_AT` & `_DW_MODEL_NAME` tracking columns |
| `safe_cast('COL', 'TYPE')` | All Models | Executes `TRY_CAST()` logic with safe fallback defaults |
| `incremental_lookback()` | Incremental Models| Applies the standard timeline boundary filter for incremental data ingestion |

---

## Contact Directory

| Functional Role | Team Responsibility |
|---|---|
| **Platform Admin** | Core Infrastructure, Terraform, Snowflake general account management |
| **Data Steward** | Enterprise Governance, Object Tagging, Masking policy enforcement |
| **Data Engineer** | Pipeline construction, dbt modeling, logical testing |
