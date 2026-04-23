# Data Processing & Transformation Walkthrough

## Executive Summary

This walkthrough document serves as the canonical technical guide detailing **exactly how and where data is transformed**, providing clear mechanical logic, logical diagrams, and practical tabular sample data mapped across the Bronze, Silver, Gold, and Semantic AI Layers.

---

## 1. System Logical Flow Architecture

This diagram illustrates the macro-level boundaries of the processing environment and the mechanisms maneuvering data between the tiered layers.

```mermaid
graph TD
    classDef bronze fill:#cd7f32,stroke:#333,stroke-width:1px,color:#fff;
    classDef silver fill:#c0c0c0,stroke:#333,stroke-width:1px,color:#000;
    classDef gold fill:#ffd700,stroke:#333,stroke-width:1px,color:#000;
    classDef ai fill:#6a0dad,stroke:#333,stroke-width:1px,color:#fff;
    
    subgraph "1. Ingestion Layer (Omnichannel)"
        A1[Kafka / Op DBs] -- Snowpipe Streaming --> B[(RAW_EVENTS)]
        A2[AWS S3 / Azure ADLS] -- Auto-Ingest Snowpipe --> B
    end

    subgraph "2. Bronze Layer (RAW_VAULT)"
        B -- "dbt: Parse JSON & Hash" --> C{{STG_ECOMMERCE_ORDERS}}
        C -- Unique Insert --> D1[(HUB_CUSTOMER)]:::bronze
        C -- Unique Insert --> D2[(HUB_ORDER)]:::bronze
        C -- Unique Combos --> D3[(LINK_CUSTOMER_ORDER)]:::bronze
        C -- SCD2 via Hash Diff --> D4[(SAT_CUSTOMER_DETAILS)]:::bronze
        C -- SCD2 via Hash Diff --> D5[(SAT_CUSTOMER_DEMOG)]:::bronze
        C -- SCD2 via Hash Diff --> D6[(SAT_ORDER_FINANCIALS)]:::bronze
    end

    subgraph "3. Silver Layer (BUSINESS_VAULT)"
        D1 -.-> E1{{DYN_PIT_CUSTOMER}}:::silver
        D4 -.-> E1
        D5 -.-> E1
        
        D1 -.-> E2{{DYN_BRIDGE_CUSTOMER_ORDERS}}:::silver
        D2 -.-> E2
        D3 -.-> E2
    end
    
    subgraph "4. Gold Layer (ANALYTICS Data Products)"
        E1 ==> F1((DP_CUSTOMER_360 View)):::gold
        E2 ==> F1
        D6 ==> F1
        E1 ==> F2((DYN_AGG_REVENUE_BY_SEGMENT)):::gold
    end
    
    subgraph "5. Semantic AI (Cortex & Snowpark)"
        F1 ==> G1((DYN_FEATURE_STORE_CUSTOMER)):::ai
        G1 -. Calculate .-> H1((PREDICT_CLV Snowpark UDF))
        G1 -. LLM Query .-> H2((AI_CLASSIFY Cortex AI))
    end
```

---

## 2. Walkthrough Scenario & Sample Data

We will practically track two user entities across two simulated timeline days to definitively demonstrate ingestion, normalization, and SCD Type 2 historic preservation.

* **Day 1**: Alice (`C100`) buys a mouse; Bob (`C200`) buys a keyboard.
* **Day 2**: Alice upgrades her loyalty tier to `PLATINUM` and changes her email address.

### Step 1: Immutable Ingestion (RAW_EVENTS)
**Location:** `RAW_VAULT.ECOMMERCE.RAW_EVENTS`  
**Tooling:** Snowpipe Streaming / AWS S3 Auto-Ingest  
**Transformation:** Strictly none. Data is structurally immutable.

| EVENT_ID | INGESTED_AT | SOURCE | RAW_JSON_PAYLOAD |
|---|---|---|---|
| `e01` | `2026-04-23 10:00` | Kafka | `{ "customer_id": "C100", "first_name": "Alice", "email": "ali@m.com", "loyalty_tier": "GOLD", "order_id": "O999", "amount": 25.0 }` |
| `e02` | `2026-04-23 10:05` | Kafka | `{ "customer_id": "C200", "first_name": "Bob", "email": "bob@m.com", "loyalty_tier": "SILVER", "order_id": "O555", "amount": 90.0 }` |
| `e03` | `2026-04-24 09:00` | S3_CRM | `{ "customer_id": "C100", "first_name": "Alice", "email": "alice_new@m.com", "loyalty_tier": "PLATINUM", "order_id": null, "amount": null }` |

---

### Step 2: Staging & Key Generation (Views)
**Location:** `RAW_VAULT.STAGING.STG_ECOMMERCE_ORDERS`  
**Tooling:** Standard dbt models via `dbt_utils.generate_surrogate_key`  
**Transformation Applied:** 
1. `PARSE_JSON` to structurally flatten incoming variant payloads.
2. Hard Typecasting (e.g., `::VARCHAR`, `::FLOAT`).
3. SHA-256 Hash String Object Generation.

**How Hashing Mathematically Operates:**
- **HK_CUSTOMER (Hash Key):** `SHA256(COALESCE(UPPER(TRIM(customer_id)), ''))`
- **HD_CUST_DEMO (Hash Diff - Demographics):** `SHA256(COALESCE(UPPER(TRIM(loyalty_tier)), '^^'))` *(Where `^^` operates as a rigid null-handling sentinel string).*

| EVENT_ID | CUSTOMER_ID | EMAIL | TIER | HK_CUSTOMER | HD_CUST_DETAILS | HD_CUST_DEMO |
|---|---|---|---|---|---|---|
| `e01` (D1) | C100 | ali@m.com | GOLD | `1a2b3c` | `x9y8z7` | `m1n2o3` |
| `e02` (D1) | C200 | bob@m.com | SILVER | `9z8y7x` | `p1q2r3` | `w9x8y7` |
| `e03` (D2) | C100 | alice_new... | PLATINUM | `1a2b3c` | `a7b8c9` | `j4k5l6` |

> [!NOTE]  
> Notice that in `e03` (Day 2), Alice's `HK_CUSTOMER` uniquely remains **`1a2b3c`** because her business identifier did not change. However, her respective Hash Diffs mathematically changed because her physical payload variables shifted.

---

### Step 3: Bronze Layer Data Vault Normalization
**Location:** `RAW_VAULT.RAW_VAULT.*`  
**Tooling:** dbt incremental loading paradigms.

#### Hubs (Core Entities)
**Rule:** Only fundamentally insert real records if the `HK` is unequivocally new.

| HK_CUSTOMER | CUSTOMER_ID | LOAD_DATETIME | RECORD_SOURCE |
|---|---|---|---|
| `1a2b3c` | C100 | `2026-04-23 10:00` | Kafka |
| `9z8y7x` | C200 | `2026-04-23 10:05` | Kafka |

> *On Day 2, record `e03` is IGNORED by the Hub process entirely because the Hash Key `1a2b3c` is already populated.*

#### Links (Transactional Relationships)
**Rule:** Define a single unique permutation of interacting Hub Hash Keys.

| HK_LINK_CUST_ORD | HK_CUSTOMER | HK_ORDER | LOAD_DATETIME |
|---|---|---|---|
| `a1b2c3` | `1a2b3c` (Alice) | `9f8e7d` (O999) | `2026-04-23 10:00` |
| `d4e5f6` | `9z8y7x` (Bob) | `5a4b3c` (O555) | `2026-04-23 10:05` |

#### Satellites (SCD Type 2 Historization)
**Rule:** Insert a new row ONLY if the incoming `HASH_DIFF` fundamentally mis-aligns with the absolute latest historical `HASH_DIFF` belonging functionally to that `HK_CUSTOMER`.

**SAT_CUSTOMER_DETAILS** (High-Velocity attribute group)

| HK_CUSTOMER | LOAD_DATETIME | HASH_DIFF | FIRST_NAME | EMAIL | STATUS |
|---|---|---|---|---|---|
| `1a2b3c` | `2026-04-23 10:00` | `x9y8z7` | Alice | ali@m.com | *Active as of Day 1* |
| `9z8y7x` | `2026-04-23 10:05` | `p1q2r3` | Bob | bob@m.com | *Active as of Day 1* |
| `1a2b3c` | `2026-04-24 09:00` | `a7b8c9` | Alice | **alice_new@m.com** | *Inserted Day 2 (Hash Diff trigger)* |

**SAT_CUSTOMER_DEMOGRAPHICS** (Low-Velocity attribute group)

| HK_CUSTOMER | LOAD_DATETIME | HASH_DIFF | LOYALTY_TIER | STATUS |
|---|---|---|---|---|
| `1a2b3c` | `2026-04-23 10:00` | `m1n2o3` | GOLD | *Active as of Day 1* |
| `9z8y7x` | `2026-04-23 10:05` | `w9x8y7` | SILVER | *Active as of Day 1* |
| `1a2b3c` | `2026-04-24 09:00` | `j4k5l6` | **PLATINUM** | *Inserted Day 2 (Hash Diff trigger)* |

---

### Step 4: Silver Layer PIT Construction Optimization
**Location:** `BUSINESS_VAULT.PIT_TABLES.*`  
**Tooling:** Snowflake Dynamic Tables (Real-time updates)  
**Transformation Phase Goal:** Traditional DV structures require computationally massive `JOIN` operations across myriad satellites to find an "active" historical slice. The `DYN_PIT_CUSTOMER` resolves this by autonomously maintaining discrete pointers structurally aligning universally disparate load times.

**DYN_PIT_CUSTOMER** (After Day 2 Updates)

| HK_CUSTOMER | CUSTOMER_ID | PIT_LOAD_DATETIME | SAT_DETAILS_LOAD_DT | SAT_DEMO_LOAD_DT |
|---|---|---|---|---|
| `1a2b3c` (Alice)| C100 | `2026-04-24 09:10` | `2026-04-24 09:00` *(Points to new email)* | `2026-04-24 09:00` *(Points to PLATINUM)* |
| `9z8y7x` (Bob)  | C200 | `2026-04-24 09:10` | `2026-04-23 10:05` *(Same pointer)* | `2026-04-23 10:05` *(Same pointer)* |

---

### Step 5: Gold Layer Analytics Output
**Location:** `ANALYTICS.SECURE_VIEWS.*`  
**Tooling:** Snowflake Secure Views controlled by native RBAC boundaries.  
**Transformation Phase Goal:** Wide, heavily denormalized output specifically organized for downstream human visualization and general purpose reporting queries.

**DP_CUSTOMER_360** (Queried explicitly by the `ANALYST` role)

| CUSTOMER_ID | FIRST_NAME | EMAIL | LOYALTY_TIER | LIFETIME_REVENUE |
|---|---|---|---|---|
| C100 | Alice | a\*\*\*@m.com | PLATINUM | 25.0 |
| C200 | Bob | b\*\*\*@m.com | SILVER | 90.0 |

> [!TIP]
> **Mechanics under the hood:**  
> When the `ANALYST` role executes a query on this view, the Snowflake optimizer reads the indexed PIT framework, generates hyper-fast `O(1)` index reads spanning the Satellite tables, performs aggregations functionally from the transaction Links, and strictly masks protected properties dynamically via `MASK_EMAIL` before any result output displays to the UI layer.

---

### Step 6: Semantic AI & ML Feature Store
**Location:** `ANALYTICS.SEMANTIC_VIEWS.*`  
**Tooling:** Snowpark Python Contexts & Snowflake Cortex LLMs  
**Transformation Process:** Rather than exporting heavy extraction loads to an outside generic ML tool, advanced analytics models run actively within Snowflake compute parameters natively against Gold data.

**DYN_FEATURE_STORE_CUSTOMER**

| CUSTOMER_ID | TIER | FEATURE_LIFETIME_REV | PREDICTED_CLV *(Snowpark ML)* | AI_CUSTOMER_CLASS *(Cortex LLM)*| AI_SUMMARY *(Cortex LLM)* |
|---|---|---|---|---|---|
| C100 | PLATINUM | 25.0 | **315.50** | `GROWTH` | "Alice's recent upgrade categorizes..."|
| C200 | SILVER | 90.0 | **110.20** | `AT_RISK` | "Bob possesses a low-growth projection..."|

**Underlying AI Operations:**
1. **PREDICT_CLV:** A Python 3.11 User-Defined Function (operating isolated within a secure Snowflake Sandboxed framework) structurally receives `(LIFETIME_REVENUE, TIER)`, runs a BG/NBD probability generation algorithm utilizing `numpy`, and outputs a predicted financial yield `FLOAT`.
2. **AI_CUSTOMER_CLASS:** The `SNOWFLAKE.CORTEX.CLASSIFY_TEXT` primitive is natively invoked, executing a zero-shot LLM prompt against concatenated historical context text strings to effectively assign an accurate categorical segment parameter.
