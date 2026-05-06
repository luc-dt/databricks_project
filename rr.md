# 🚲 Databricks Bike Data Lakehouse Project

![CI](<ci-badge-url>)
![Python](<python-badge-url>)
![Databricks](<databricks-badge-url>)
![Delta Lake](<delta-badge-url>)
![Linter](<linter-badge-url>)

Short project intro (1–2 sentences).

## 📖 Overview
- **Bronze:** ...
- **Silver:** ...
- **Gold:** ...
- **Pipeline:** ...

![Medallion Architecture](script/images/architecture.png)

---

## 🗂️ Repository Structure
```text
script/
├── init_lakehouse.ipynb
├── utils/
│   └── config.ipynb
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
