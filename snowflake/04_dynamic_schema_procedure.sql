-- =============================================================================
-- 04_dynamic_schema_procedure.sql — Dynamic Schema Inference
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Snowpark Python stored procedure that infers schema from staged
--          files (CSV, JSON, Parquet) and registers it in SCHEMA_REGISTRY.
--          Enables automated schema drift detection.
--
-- EXECUTION ORDER: Run AFTER 03_metadata_and_control_tables.sql
-- REQUIRES: PLATFORM_ADMIN, Snowpark Python runtime
-- =============================================================================

USE ROLE PLATFORM_ADMIN;
USE DATABASE RAW_VAULT;
USE SCHEMA ECOMMERCE;

-- =============================================================================
-- 1. INFER_STAGE_SCHEMA — Snowpark Python Procedure
-- =============================================================================
CREATE OR REPLACE PROCEDURE RAW_VAULT.ECOMMERCE.INFER_STAGE_SCHEMA(
    STAGE_NAME      VARCHAR,
    SOURCE_SYSTEM   VARCHAR,
    TABLE_NAME      VARCHAR,
    FILE_FORMAT     VARCHAR DEFAULT 'FF_CSV_COMMA',
    SAMPLE_ROWS     INT     DEFAULT 1000
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'infer_schema'
COMMENT = 'Infers schema from staged files, registers in SCHEMA_REGISTRY, detects drift'
AS
$$
import json
from datetime import datetime

def infer_schema(session, stage_name: str, source_system: str, table_name: str,
                 file_format: str = 'FF_CSV_COMMA', sample_rows: int = 1000) -> dict:
    """
    Infer schema from a Snowflake external stage and register it in the SCHEMA_REGISTRY.

    Steps:
        1. Use INFER_SCHEMA() to extract column metadata from staged files
        2. Compare against current schema in SCHEMA_REGISTRY
        3. Detect drift (new columns, removed columns, type changes)
        4. Insert new schema version if changes detected
        5. Return a summary of inferred columns and any drift

    Args:
        stage_name:    Fully qualified stage name (e.g., RAW_VAULT.ECOMMERCE.STG_S3_ECOMMERCE)
        source_system: Source identifier (e.g., ECOMMERCE, CRM)
        table_name:    Target table name
        file_format:   File format name for parsing
        sample_rows:   Number of rows to sample for inference

    Returns:
        dict with 'columns_inferred', 'drift_detected', 'changes'
    """
    result = {
        'source_system': source_system,
        'table_name': table_name,
        'stage_name': stage_name,
        'columns_inferred': 0,
        'drift_detected': False,
        'changes': [],
        'schema_version': 1,
        'timestamp': datetime.utcnow().isoformat()
    }

    # Step 1: Infer schema from staged files
    infer_sql = f"""
        SELECT *
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@{stage_name}',
                FILE_FORMAT => '{file_format}',
                MAX_RECORDS_PER_FILE => {sample_rows}
            )
        )
    """

    try:
        inferred_df = session.sql(infer_sql).collect()
    except Exception as e:
        result['error'] = f"Schema inference failed: {str(e)}"
        return result

    inferred_columns = {}
    for row in inferred_df:
        col_name = row['COLUMN_NAME']
        col_type = row['TYPE']
        col_nullable = row.get('NULLABLE', True)
        col_ordinal = row.get('ORDER_ID', 0)
        inferred_columns[col_name] = {
            'data_type': col_type,
            'is_nullable': col_nullable,
            'ordinal_position': col_ordinal
        }

    result['columns_inferred'] = len(inferred_columns)

    # Step 2: Fetch current schema from registry
    current_schema_sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, SCHEMA_VERSION
        FROM AUDIT.CONTROL.SCHEMA_REGISTRY
        WHERE SOURCE_SYSTEM = '{source_system}'
          AND TABLE_NAME = '{table_name}'
          AND IS_CURRENT = TRUE
        ORDER BY ORDINAL_POSITION
    """

    current_rows = session.sql(current_schema_sql).collect()
    current_columns = {}
    current_version = 0

    for row in current_rows:
        current_columns[row['COLUMN_NAME']] = row['DATA_TYPE']
        current_version = max(current_version, row['SCHEMA_VERSION'])

    # Step 3: Detect drift
    changes = []

    # New columns
    for col_name, col_info in inferred_columns.items():
        if col_name not in current_columns:
            changes.append({
                'column_name': col_name,
                'change_type': 'ADD',
                'new_type': col_info['data_type'],
                'old_type': None
            })
        elif col_info['data_type'] != current_columns[col_name]:
            changes.append({
                'column_name': col_name,
                'change_type': 'TYPE_CHANGE',
                'new_type': col_info['data_type'],
                'old_type': current_columns[col_name]
            })

    # Removed columns
    for col_name in current_columns:
        if col_name not in inferred_columns:
            changes.append({
                'column_name': col_name,
                'change_type': 'REMOVE',
                'new_type': None,
                'old_type': current_columns[col_name]
            })

    result['drift_detected'] = len(changes) > 0
    result['changes'] = changes

    # Step 4: Register schema (new version if drift detected, or first registration)
    new_version = current_version + 1 if changes else max(current_version, 1)
    result['schema_version'] = new_version

    if changes or not current_columns:
        # Mark current version as non-current
        if current_columns:
            session.sql(f"""
                UPDATE AUDIT.CONTROL.SCHEMA_REGISTRY
                SET IS_CURRENT = FALSE
                WHERE SOURCE_SYSTEM = '{source_system}'
                  AND TABLE_NAME = '{table_name}'
                  AND IS_CURRENT = TRUE
            """).collect()

        # Insert new schema version
        for col_name, col_info in inferred_columns.items():
            change_type = None
            prev_type = None
            for chg in changes:
                if chg['column_name'] == col_name:
                    change_type = chg['change_type']
                    prev_type = chg['old_type']
                    break

            session.sql(f"""
                INSERT INTO AUDIT.CONTROL.SCHEMA_REGISTRY
                    (SOURCE_SYSTEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
                     ORDINAL_POSITION, IS_NULLABLE, SCHEMA_VERSION,
                     IS_CURRENT, PREVIOUS_DATA_TYPE, CHANGE_TYPE)
                VALUES
                    ('{source_system}', '{table_name}', '{col_name}', '{col_info["data_type"]}',
                     {col_info["ordinal_position"]}, {col_info["is_nullable"]}, {new_version},
                     TRUE, {f"'{prev_type}'" if prev_type else 'NULL'},
                     {f"'{change_type}'" if change_type else 'NULL'})
            """).collect()

    return result
$$;

-- =============================================================================
-- 2. GRANTS
-- =============================================================================
GRANT USAGE ON PROCEDURE RAW_VAULT.ECOMMERCE.INFER_STAGE_SCHEMA(
    VARCHAR, VARCHAR, VARCHAR, VARCHAR, INT
) TO ROLE DATA_ENGINEER;
GRANT USAGE ON PROCEDURE RAW_VAULT.ECOMMERCE.INFER_STAGE_SCHEMA(
    VARCHAR, VARCHAR, VARCHAR, VARCHAR, INT
) TO ROLE TRANSFORMER;

-- =============================================================================
-- 3. EXAMPLE USAGE
-- =============================================================================
-- CALL RAW_VAULT.ECOMMERCE.INFER_STAGE_SCHEMA(
--     'RAW_VAULT.ECOMMERCE.STG_S3_ECOMMERCE',
--     'ECOMMERCE',
--     'CUSTOMERS',
--     'FF_CSV_COMMA',
--     1000
-- );
