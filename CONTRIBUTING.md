# CONTRIBUTING.md — Data Vault 2.0 Snowflake Data Platform

## Development Process

1. **Branch** from `develop` using `feature/{ticket-id}/{description}`
2. **Develop** locally against `dev` target
3. **Test** with `dbt test --select +your_model+ --profiles-dir .`
4. **Push** and open a PR against `develop`
5. **CI** automatically runs SQLFluff lint + Slim CI build + tests
6. **Review** by at least 1 peer (Data Engineer or Platform Admin)
7. **Merge** to `develop` → staging deploy → validate → merge to `main`

## Code Standards

### SQL Style
- All SQL keywords UPPERCASE
- 4-space indentation
- Trailing commas in SELECT lists
- Table aliases: 3-letter abbreviations (`cus`, `ord`, `prd`)
- CTE names: descriptive, snake_case

### Naming Conventions
See [Data Vault 2.0 Guide](docs/data_vault_guide.md#naming-conventions)

### Testing Requirements
- Every Hub: `unique` + `not_null` on hash key and business key
- Every Link: `unique` on link hash key + `not_null` on FK hash keys
- Every Satellite: `not_null` on hash key, hash diff, load datetime
- Every Fact: Range tests on measures (no negative amounts unless justified)
- Every Dimension: `unique` on surrogate key

## Pre-Commit Hooks

This project uses `pre-commit` for automated checks:
```bash
pre-commit install
pre-commit run --all-files
```

Hooks: SQLFluff, YAML lint, trailing whitespace, large file detection.
