# 📊 Databricks Sales Data Warehouse  
### Lakeflow Spark Declarative Pipelines (SDP)

This project implements an **end-to-end Sales Data Warehouse** using **Lakeflow Spark Declarative Pipelines (SDP)** on Databricks. The solution follows the **Medallion Architecture (Bronze → Silver → Gold)** to process regional sales data, manage dimensions, and deliver analytics-ready business insights.

---
## Lakeflow 
<img width="400" height="246" alt="Image" src="https://github.com/user-attachments/assets/c515f102-6fb3-4ac8-af53-c503302e8ec8" />

## 🏗️ Architecture Overview

The pipeline is built using a **declarative framework** that automatically manages dependencies, execution order, and data lineage.

### 🥉 Bronze Layer – Ingestion
- Ingests raw data from multiple regional sources (Sales East / Sales West)
- Ingests Product and Customer dimension data
- Uses the `append_flow` API to consolidate raw data into Bronze tables

### 🥈 Silver Layer – Transformation & CDC
- Performs data enrichment and standardization
- Implements **SCD Type 1** upserts to maintain the latest state of records
- Uses `create_auto_cdc_flow()` to handle Change Data Capture (CDC)
- Eliminates the need for manual `MERGE` logic

### 🥇 Gold Layer – Analytics & History
- Implements **SCD Type 2** for historical tracking of dimensional changes
- Automatically manages history columns (`__START_AT`, `__END_AT`)
- Builds curated fact tables and business-level aggregated views

---

## 🚀 Key Features

- 🔄 **Automated CDC Logic**  
  Simplifies Change Data Capture using the `create_auto_cdc_flow` API instead of complex SQL merges.

- ✅ **Data Quality Expectations**  
  Built-in expectations enforce data integrity by warning, dropping, or failing the pipeline  
  (e.g., non-null IDs, valid prices).

- 🔁 **Unified Batch & Streaming Processing**  
  The same pipeline logic supports both streaming ingestion and batch backfills.

- 🕒 **SCD Type 2 History Tracking**  
  Automatically tracks record changes over time without custom SCD logic.

- 🧑‍💻 **Lakeflow Code Editor**  
  Uses the Lakeflow pipeline editor for clean, folder-based, and maintainable pipeline development.

---
## 🛠️ Prerequisites & Setup

1. **Databricks Premium Workspace**  
   Lakeflow Spark Declarative Pipelines are available in Premium workspaces.

2. **Enable Lakeflow Pipeline Editor**  
   - Navigate to *Developer Settings*
   - Enable **Lakeflow pipeline editor**

3. **Unity Catalog**  
   - A catalog and schema must exist
   - Required for streaming tables and materialized views

4. **Compute**
   - Serverless compute or SQL Warehouse
   - Used for transformations and metadata management

---

## 🧩 Core API Reference: `create_auto_cdc_flow`

Used in the Silver and Gold layers to process data from **Change Data Feed (CDF)** sources.

| Parameter | Description |
|--------|------------|
| `target` | Target streaming table to be updated |
| `keys` | Primary keys used for upsert matching |
| `sequence_by` | Column that defines the logical order of changes |
| `stored_as_scd_type` | `1` for overwrite, `2` for historical tracking |
| `expect_all_or_drop` | Enforces data quality rules within the flow |

---

## 📊 Business Insights

The final **Gold layer** exposes a **materialized business view** that:
- Aggregates **Total Sales**
- Groups results by **Region** and **Product Category**
- Enables stakeholders to quickly identify high-performing markets

---

## 🎯 Why Lakeflow Spark Declarative Pipelines?

- Minimal boilerplate code
- Built-in CDC and SCD support
- Automatic dependency management and lineage
- Production-ready pipelines with improved reliability and maintainability

---

