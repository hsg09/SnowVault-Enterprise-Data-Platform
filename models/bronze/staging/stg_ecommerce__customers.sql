{{/*
  stg_ecommerce__customers.sql — Pre-Vault Staging: Customers
  
  PURPOSE: Transform raw customer data into Data Vault-ready format.
           - Generate SHA-256 hash keys (HK_CUSTOMER)
           - Generate hash diff for satellite change detection
           - Add record source + load datetime metadata
           - Standardise data types and handle nulls
*/}}

WITH source AS (

    SELECT * FROM {{ source('raw_ecommerce', 'RAW_CUSTOMERS') }}

),

renamed AS (

    SELECT
        -- ===== Business Key =====
        TRIM(UPPER(CUSTOMER_ID))                        AS CUSTOMER_ID,

        -- ===== Descriptive Attributes (Satellite: Details) =====
        TRIM(INITCAP(FIRST_NAME))                       AS FIRST_NAME,
        TRIM(INITCAP(LAST_NAME))                        AS LAST_NAME,
        TRIM(LOWER(EMAIL))                              AS EMAIL,
        TRIM(PHONE)                                     AS PHONE,
        TRIM(UPPER(COUNTRY_CODE))                       AS COUNTRY_CODE,
        TRIM(INITCAP(CITY))                             AS CITY,
        TRIM(UPPER(STATE))                              AS STATE,
        TRIM(POSTAL_CODE)                               AS POSTAL_CODE,
        REGISTRATION_DATE,

        -- ===== Descriptive Attributes (Satellite: Demographics) =====
        DATE_OF_BIRTH,
        TRIM(UPPER(GENDER))                             AS GENDER,
        TRIM(UPPER(CUSTOMER_SEGMENT))                   AS CUSTOMER_SEGMENT,
        TRIM(UPPER(LOYALTY_TIER))                       AS LOYALTY_TIER,

        -- ===== Metadata =====
        _LOADED_AT,
        _FILE_NAME,
        _FILE_ROW_NUMBER

    FROM source

),

hashed AS (

    SELECT
        *,

        -- ===== Hash Key (Hub) =====
        {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }}
                                                        AS HK_CUSTOMER,

        -- ===== Hash Diff (Satellite: Customer Details) =====
        {{ dbt_utils.generate_surrogate_key([
            'FIRST_NAME',
            'LAST_NAME',
            'EMAIL',
            'PHONE',
            'COUNTRY_CODE',
            'CITY',
            'STATE',
            'POSTAL_CODE',
            'REGISTRATION_DATE'
        ]) }}                                           AS HD_CUSTOMER_DETAILS,

        -- ===== Hash Diff (Satellite: Customer Demographics) =====
        {{ dbt_utils.generate_surrogate_key([
            'DATE_OF_BIRTH',
            'GENDER',
            'CUSTOMER_SEGMENT',
            'LOYALTY_TIER'
        ]) }}                                           AS HD_CUSTOMER_DEMOGRAPHICS,

        -- ===== Load Metadata =====
        _LOADED_AT                                      AS LOAD_DATETIME,
        'ECOMMERCE.RAW_CUSTOMERS'                       AS RECORD_SOURCE,
        CURRENT_DATE()                                  AS APPLIED_DATE

    FROM renamed

)

SELECT * FROM hashed
