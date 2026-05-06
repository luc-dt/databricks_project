 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/README.md b/README.md
index 6daed9bf43cb58e8591b46fb60ca66d387f2d068..a1b1bed1f3e0a4a9f1e18b67c77ec2894acf8189 100644
--- a/README.md
+++ b/README.md
@@ -77,50 +77,97 @@ Run the ingestion notebook:
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
 
 ![Data Flow Diagram](script/images/diagram.png)
 
+### ERD (Code-Aligned)
+```mermaid
+erDiagram
+    DIM_CUSTOMERS ||--o{ FACT_SALES : "customer_key"
+    DIM_PRODUCTS  ||--o{ FACT_SALES : "product_key"
+
+    DIM_CUSTOMERS {
+        int customer_key PK
+        string customer_id
+        string customer_number
+        string first_name
+        string last_name
+        string country
+        string marital_status
+        string gender
+        date birthdate
+        date create_date
+    }
+
+    DIM_PRODUCTS {
+        int product_key PK
+        string product_id
+        string product_number
+        string product_name
+        string category_id
+        string category
+        string subcategory
+        string maintenance_flag
+        string product_line
+        date start_date
+    }
+
+    FACT_SALES {
+        string order_number
+        int product_key FK
+        int customer_key FK
+        date order_date
+        date ship_date
+        date due_date
+        float sales_amount
+        int quantity
+        float price
+    }
+```
+
+> Note: Composite business key `(order_number, product_number)` is enforced in Silver `crm_sales`. Gold `fact_sales` links to dimensions via surrogate keys.
+
 **Silver Outputs**
 The Silver layer produces cleaned, standardized tables such as:
 *   `workspace.silver.crm_customers`
 *   `workspace.silver.crm_products`
 *   `workspace.silver.crm_sales`
 *   `workspace.silver.erp_customers`
 *   `workspace.silver.erp_customer_location`
 *   `workspace.silver.erp_product_category`
 
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
 
@@ -183,51 +230,65 @@ TABLES = {
 Centralizing table references means environment promotion (dev → prod) requires changing one file, not touching every pipeline notebook.
 
 ### 4. 📡 Monitoring & Observability
 Every Silver notebook writes one audit row to a dedicated monitoring table after each pipeline run. This makes pipeline health visible and queryable.
 
 **Audit log table:** `workspace.monitoring.pipeline_audit_log`
 
 | Column | Type | Description |
 |---|---|---|
 | `notebook_name` | STRING | Which notebook ran |
 | `target_table` | STRING | Which silver table was written |
 | `run_timestamp` | TIMESTAMP | When the run completed |
 | `rows_inserted` | LONG | Rows added by MERGE |
 | `rows_updated` | LONG | Rows modified by MERGE |
 | `rows_deleted` | LONG | Rows removed by MERGE |
 | `qc_status` | STRING | `PASS` or `FAIL` |
 | `qc_message` | STRING | Failure reason or `All checks passed` |
 
 QC asserts are wrapped in `try/except AssertionError` so the audit log is always written — whether the pipeline passes or fails. A pipeline that crashes silently is unobservable; a pipeline that logs its failure is debuggable.
 
 Each Silver notebook includes a final sanity check cell that displays the audit log sorted by most recent runs first:
 ```python
 spark.sql(f"SELECT * FROM {TABLES['audit_log']} ORDER BY run_timestamp DESC").display()
 ```
 
-### 5. 🧪 CI/CD & Automated Testing
+
+### 5. 🧭 Diagram-to-Code Alignment Notes
+To keep portfolio diagrams fully aligned with current implementation:
+
+- **ERD columns follow Gold SQL outputs exactly**:
+  - `dim_customers`: includes `customer_key`, `customer_id`, `customer_number`, `first_name`, `last_name`, `country`, `marital_status`, `gender`, `birthdate`, `create_date`.
+  - `dim_products`: includes `product_key`, `product_id`, `product_number`, `product_name`, `category_id`, `category`, `subcategory`, `maintenance_flag`, `product_line`, `start_date`.
+- **Key semantics clarified**:
+  - Composite business key `(order_number, product_number)` is enforced in **Silver** `crm_sales`.
+  - **Gold** `fact_sales` links via surrogate keys (`customer_key`, `product_key`) for dimensional joins.
+- **SCD2 messaging in Gold**:
+  - `dim_products` now filters to active records (`WHERE end_date IS NULL`) so the Gold dimension reflects current active product rows.
+  - `dim_customers` remains an enriched current-state dimension (not full SCD2 history output).
+
+### 6. 🧪 CI/CD & Automated Testing
 A continuous integration pipeline and automated test suite ensure the configuration integrity and code quality across all pipeline notebooks.
 
 **Why This Matters**
 
 In a production lakehouse, configuration drift is a silent killer. A typo in a schema name, a missing table key in `TABLES`, or an accidental hardcoded catalog reference can break an entire downstream workflow — often only discovered during a late-night production run. Automated testing catches these issues before they reach production.
 
 Additionally, notebook code is notoriously difficult to lint and test compared to plain Python files. The CI/CD setup bridges this gap by treating notebooks as testable, lintable artifacts.
 
 **What Was Implemented**
 
 Three components work together to enforce quality gates on every push:
 
 **1. GitHub Actions Workflow** (`.github/workflows/pipeline_ci.yml`)
 
 Triggers automatically on every push or pull request to `main`. The workflow performs:
 
 *   **Code Quality Check**: Runs `nbqa ruff` to lint all notebooks using Ruff (a fast Python linter). This catches PEP 8 violations, unused imports, undefined variables, and other code smells directly inside `.ipynb` files.
 *   **Automated Tests**: Executes the pytest test suite against the centralized config notebook to validate configuration integrity.
 
 ```yaml
 name: PySpark CI
 
 on:
   push:
     branches: [main]
diff --git a/requirements-dev.txt b/requirements-dev.txt
index 274187aacea4ef776cd1c5215fcc3c6688b4bbe3..429e83b443b0313ccf47cc4c770e668356ddde23 100644
--- a/requirements-dev.txt
+++ b/requirements-dev.txt
@@ -1,4 +1,4 @@
 nbqa==1.9.1
 ruff==0.4.4
 pytest==8.2.0
-nbformat==5.10.4.
\ No newline at end of file
+nbformat==5.10.4
\ No newline at end of file
diff --git a/script/gold/gold_dim_products.ipynb b/script/gold/gold_dim_products.ipynb
index f62ad740d109c4e3a8157c308d82ae54922ceb1c..46807d9d1bdfd0ac204911356f39429215e333de 100644
--- a/script/gold/gold_dim_products.ipynb
+++ b/script/gold/gold_dim_products.ipynb
@@ -30,51 +30,51 @@
      "nuid": "408acf60-737a-4685-a370-d3f10828a51d",
      "showTitle": false,
      "startTime": 1777539498356,
      "submitTime": 1777539498310,
      "tableResultSettingsMap": {},
      "title": ""
     }
    },
    "outputs": [],
    "source": [
     "query = \"\"\"\n",
     "SELECT\n",
     "    ROW_NUMBER() OVER (ORDER BY pn.start_date, pn.product_number) AS product_key, -- Surrogate key\n",
     "    pn.product_id,\n",
     "    pn.product_number,\n",
     "    pn.product_name,\n",
     "    pn.category_id,\n",
     "    pc.category,\n",
     "    pc.subcategory,\n",
     "    pc.maintenance_flag,\n",
     "    pn.product_line,\n",
     "    pn.start_date\n",
     "FROM silver.crm_products pn\n",
     "LEFT JOIN silver.erp_product_category pc\n",
     "    ON pn.category_id = pc.category_id\n",
-    "--WHERE pn.end_date IS NULL; -- Filter out all historical data\n",
+    "WHERE pn.end_date IS NULL -- Keep active SCD2 record only\n",
     "\"\"\"\n",
     "df = spark.sql(query)"
    ]
   },
   {
    "cell_type": "code",
    "execution_count": 0,
    "metadata": {
     "application/vnd.databricks.v1+cell": {
      "cellMetadata": {
       "byteLimit": 2048000,
       "rowLimit": 10000
      },
      "finishTime": 1777539512248,
      "inputWidgets": {},
      "nuid": "cc8145e2-ef32-4b12-a3c7-afb312935952",
      "showTitle": false,
      "startTime": 1777539504764,
      "submitTime": 1777539504728,
      "tableResultSettingsMap": {},
      "title": ""
     }
    },
    "outputs": [],
    "source": [
@@ -186,26 +186,26 @@
    "dashboards": [],
    "environmentMetadata": {
     "base_environment": "",
     "environment_version": "5"
    },
    "inputWidgetPreferences": null,
    "language": "python",
    "notebookMetadata": {
     "mostRecentlyExecutedCommandWithImplicitDF": {
      "commandId": 8419362217277058,
      "dataframes": [
       "_sqldf"
      ]
     },
     "pythonIndentUnit": 4
    },
    "notebookName": "gold_dim_products",
    "widgets": {}
   },
   "language_info": {
    "name": "python"
   }
  },
  "nbformat": 4,
  "nbformat_minor": 0
-}
+}
\ No newline at end of file
 
EOF
)
