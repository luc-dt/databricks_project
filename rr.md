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
 
EOF
)
