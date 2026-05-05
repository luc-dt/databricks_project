# 🚲 Databricks Bike Data Lakehouse Project

This repository documents an end-to-end Databricks Lakehouse built with the **Medallion Architecture**. It ingests raw CRM and ERP files, cleans and standardizes them, models them into a star schema, and prepares the workflow for orchestration in Databricks.

## 📖 Overview
The project is organized into three data layers and one automation layer. The final goal is to produce reliable analytics tables for reporting and BI:

*   **Bronze:** Raw ingestion with no transformations.
*   **Silver:** Cleansing, standardization, and validation.
*   **Gold:** Business-ready dimensional modeling.
*   **Pipeline:** Orchestration with Databricks Workflows.

---

## 🗂️ Repository Structure
```text
script/
├── init_lakehouse.ipynb
├── utils/
│   └── config.ipynb            # Centralized config — schemas, table registry
├── bronze/
│   └── bronze_layer.ipynb
├── silver/
│   ├── silver_orchestration.ipynb
│   ├── crm/
│   │   ├── silver_crm_cust_info.ipynb
│   │   ├── silver_crm_prd_info.ipynb
│   │   └── silver_crm_sales_details.ipynb
│   └── erp/
│       ├── silver_erp_cust_az12.ipynb
│       ├── silver_erp_loc_a101.ipynb
│       └── silver_erp_px_cat_g1v2.ipynb
└── gold/
    ├── gold_orchestration.ipynb
    ├── gold_dim_customers.ipynb
    ├── gold_dim_products.ipynb
    └── gold_fact_sales.ipynb
```

---

## ⚙️ Setup Instructions

**1. Prepare the workspace**
Use a Databricks workspace with Unity Catalog enabled. Create or confirm the following schemas:
*   `bronze`
*   `silver`
*   `gold`

**2. Create the raw file volume**
Create a volume in the Bronze schema to act as the landing zone:
`workspace.bronze.raw_sources`

**3. Upload source data**
Place the six source CSV files into the `raw_sources` volume.

**4. Initialize the lakehouse**
Run the initialization script. This notebook prepares the schemas and storage needed for the project.
```bash
script/init_lakehouse.ipynb
```

---

## 🚀 Execution Flow

### 🥉 Bronze Layer
Run the ingestion notebook:
```bash
script/bronze/bronze_layer.ipynb
```
This notebook reads each source CSV and writes it as a Delta table in `workspace.bronze` using a consistent naming convention.

### 🥈 Silver Layer
Run the orchestration notebook:
```bash
script/silver/silver_orchestration.ipynb
```
This notebook triggers the Silver transformation notebooks in sequence. The Silver layer cleans, standardizes, and validates data before writing to `workspace.silver`.

### 🥇 Gold Layer
Run the dimensional modeling notebook:
```bash
script/gold/gold_orchestration.ipynb
```
This notebook builds the dimensional model in `workspace.gold`, including customer, product, and sales tables for analytics.

---

## 📊 Data Model Outputs

**Silver Outputs**
The Silver layer produces cleaned, standardized tables such as:
*   `workspace.silver.crm_customers`
*   `workspace.silver.crm_products`
*   `workspace.silver.crm_sales`
*   `workspace.silver.erp_cust_az12`
*   `workspace.silver.erp_loc_a101`
*   `workspace.silver.erp_px_cat_g1v2`

**Gold Outputs**
The Gold layer creates the final Star Schema:
*   `workspace.gold.dim_customers`
*   `workspace.gold.dim_products`
*   `workspace.gold.fact_sales`

---

## 🔄 Pipeline Automation
The pipeline is designed to run as a Databricks Workflow named `loading_bike_data_lakehouse`.

**Recommended Task Order:**
1.  Bronze ingestion
2.  Silver orchestration
3.  Gold orchestration

---

## ✅ Production-Grade Enhancements

Beyond the base Lakehouse, three production engineering patterns were implemented:

### 1. 📋 Data Quality Framework
Every Silver notebook includes a validation block that runs after each write. Checks enforced:

| Check | Rule |
|---|---|
| Row count | `total_rows > 0` — table must not be empty |
| Null PKs | `null_pk == 0` — primary key must be fully populated |
| Duplicate PKs | `duplicate_pk == 0` — no duplicate primary keys allowed |
| Business rules | e.g. `sales_amount > 0` on `crm_sales` |

QC is intentionally placed at the **Silver layer**, not Gold. Gold is a transformation layer — if corrupt data reaches Gold, failures are silent. Silver is the last point where raw data shape can be enforced.

### 2. 🔁 Incremental MERGE Strategy
Silver writes use `MERGE INTO` (upsert) instead of full overwrite. This means each pipeline run only touches rows that are new or changed — leaving unaffected rows untouched.

```python
# Pattern used across all 6 silver notebooks
if not spark.catalog.tableExists(TARGET_TABLE):
    df.write.format("delta").saveAsTable(TARGET_TABLE)   # first run
else:
    delta_target = DeltaTable.forName(spark, TARGET_TABLE)
    delta_target.alias("target").merge(
        df.alias("source"),
        f"target.{PK_COL} = source.{PK_COL}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

`crm_sales` uses a **composite key** `(order_number, product_number)` because no single column uniquely identifies a row in that table.

### 3. ⚙️ Centralized Configuration
All Silver and Gold notebooks source table names and schema references from a single config notebook (`utils/config`), loaded via `%run`:

```python
CATALOG       = "workspace"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

TABLES = {
    "crm_cust":  f"{CATALOG}.{SILVER_SCHEMA}.crm_customers",
    "crm_prd":   f"{CATALOG}.{SILVER_SCHEMA}.crm_products",
    "crm_sales": f"{CATALOG}.{SILVER_SCHEMA}.crm_sales",
    "erp_cust":  f"{CATALOG}.{SILVER_SCHEMA}.erp_cust_az12",
    "erp_loc":   f"{CATALOG}.{SILVER_SCHEMA}.erp_loc_a101",
    "erp_cat":   f"{CATALOG}.{SILVER_SCHEMA}.erp_px_cat_g1v2",
}
```

Centralizing table references means environment promotion (dev → prod) requires changing one file, not touching every pipeline notebook.

---

## 🎯 Purpose & Usage
This project demonstrates a practical Databricks Lakehouse implementation using:
*   Medallion Architecture (Bronze / Silver / Gold)
*   Unity Catalog governance
*   Delta tables with ACID guarantees
*   Incremental MERGE pipelines
*   Data Quality validation at the Silver layer
*   Config-driven, environment-portable notebook design
*   Dimensional modeling (Star Schema)
*   Workflow orchestration via Databricks Jobs