 # 🛒 E-Commerce ETL Data Warehouse & Analytics Dashboard

<div align="center">

![Python](https://img.shields.io/badge/Python-3.14.2-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791.svg)
![Power BI](https://img.shields.io/badge/Power_BI-Desktop-F2C811.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

**An end-to-end ETL data pipeline transforming Brazilian e-commerce data into actionable business insights**

</div>

---

## 🎯 Overview

This project demonstrates a complete data engineering workflow, from raw data extraction to interactive business intelligence dashboards. Built using real-world Brazilian e-commerce data from Olist, the project implements industry best practices in ETL development, dimensional modeling, and data visualization.

### **Project Highlights**

- ✅ **550,000+ records** processed across 8 source datasets
- ✅ **Star schema data warehouse** with 99,441 fact records
- ✅ **25+ interactive visualizations** across 4 dashboard pages
- ✅ **99.8% data quality** with full referential integrity
- ✅ **Modular ETL pipeline** with comprehensive logging
- ✅ **Cohort retention analysis** for customer behavior insights

### **Business Value**

- 📊 360° customer view with lifetime value segmentation
- 📈 Real-time operational performance monitoring
- 🎯 Data-driven product and geographic revenue analysis
- 🔄 Customer retention tracking with cohort-based analytics

---

## ✨ Key Features

### **ETL Pipeline**
- Automated data extraction from multiple CSV sources
- Complex data transformations using pandas
- Surrogate key generation for dimensional modeling
- Incremental loading with error handling
- Comprehensive data quality validation

### **Data Warehouse**
- Star schema design optimized for analytics
- PostgreSQL database with proper indexing
- Foreign key relationships ensuring referential integrity
- Slowly Changing Dimension (SCD Type 1) support
- Fact and dimension tables with 132,000+ records

### **Analytics Dashboard**
- 4 interactive pages: Executive Summary, Sales Performance, Customer Analytics, Product Analytics
- 15+ DAX measures for business metrics
- Cross-filtering and drill-through capabilities
- Geographic visualizations with Brazilian state mapping
- Cohort retention heatmap with conditional formatting

---

## 🛠 Tech Stack

### **Core Technologies**

| Category | Technology | Purpose |
|----------|-----------|---------|
| **Language** | Python 3.14.2 | ETL scripting and data transformation |
| **Database** | PostgreSQL 18 | Data warehouse storage |
| **BI Tool** | Power BI Desktop | Data visualization and dashboards |
| **Query Language** | SQL | Database schema and analytics queries |
| **Formula Language** | DAX | Power BI measures and calculations |

### **Python Libraries**

```python
pandas==2.2.0          # Data manipulation and transformation
psycopg2-binary==2.9.9        # PostgreSQL database adapter
python-dotenv==1.0.0   # Environment variable management
```

### **Additional Tools**
- **pgAdmin 4** - Database administration
- **Git/GitHub** - Version control
- **VS Code** - Development environment

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES (Kaggle)                      │
│   Orders | Customers | Products | Payments | Reviews | Sellers  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PHASE 1: EXTRACTION                          │
│              src/extract/extract_data.py                        │
│   -  Validate source files  -  Quality checks  -  Copy to raw/  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PHASE 2: TRANSFORMATION                       │
│             src/transform/transform_data.py                     │
│   -  Star schema design  -  Surrogate keys  -  Business logic   │
│   -  Data cleansing  -  Aggregations  -  Cohort analysis        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 PHASE 3: DATABASE SETUP                         │
│              src/schema/create_schema.py                        │
│   -  DDL scripts  -  Table creation  -  Constraints  -  Indexes │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              PHASE 4 & 5: DATA LOADING                          │
│   src/load/load_dimensions.py | src/load/load_facts.py          │
│   -  Bulk insert operations  -  FK validation  -  Error handling│
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PostgreSQL Data Warehouse                     │
│         ecommerce_dw (6 tables, 132K+ records)                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              PHASE 6: VISUALIZATION                             │
│            Power BI Dashboard (4 pages, 25+ visuals)            │
│   -  DAX measures  -  Interactive filters  -  Cross-filtering   │
└─────────────────────────────────────────────────────────────────┘
```

---

### **Table Descriptions**

| Table | Type | Records | Description |
|-------|------|---------|-------------|
| **fact_orders** | Fact | 99,441 | Order transactions with metrics (revenue, delivery, reviews) |
| **fact_cohort_retention** | Fact | 1,077 | Customer retention rates by cohort |
| **dim_date** | Dimension | 1,826 | Date dimension (2016-2020) with time attributes |
| **dim_products** | Dimension | 32,951 | Product catalog with categories |
| **dim_customers** | Dimension | 99,441 | Customer profiles with CLV and segments |
| **dim_payment_type** | Dimension | 5 | Payment method lookup table |

---

## 📸 Dashboard Preview

### 1️⃣ Executive Summary
<details>
<summary>Click to expand</summary>
<img width="1284" height="720" alt="Screenshot 2026-01-29 142446" src="https://github.com/user-attachments/assets/20b4c711-2276-4250-844a-332933eb63b8" />

**Key Metrics:**
- Total Revenue: R$ 15.42M
- Completed Orders: 96K
- Total Customers: 99K
- Average Order Value: R$ 159.33

</details>

