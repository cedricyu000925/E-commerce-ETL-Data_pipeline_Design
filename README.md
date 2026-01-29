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

### 2️⃣ Sales Performance
<details>
<summary>Click to expand</summary>

<img width="1280" height="721" alt="Screenshot 2026-01-29 145036" src="https://github.com/user-attachments/assets/8e7addc1-bbbc-4fef-969e-70f98203ad59" />


**Key Metrics:**
- Order Completion Rate: 97%
- Average Delivery Days: 11.73
- Late Delivery Rate: 6.8%
- On-Time Delivery Rate: 93.2%

</details>

### 3️⃣ Customer Analytics
<details>
<summary>Click to expand</summary>

<img width="1280" height="721" alt="Screenshot 2026-01-29 145036" src="https://github.com/user-attachments/assets/45e84381-3eda-48e3-9e08-97246109699e" />

**Key Metrics:**
- Total Customers: 96.1K
- Average CLV: R$ 166
- New Customers: 22.3K

</details>

### 4️⃣ Product Analytics
<details>
<summary>Click to expand</summary>

<img width="1280" height="720" alt="Screenshot 2026-01-29 150956" src="https://github.com/user-attachments/assets/c274993f-1e50-44e0-b522-eaaf2c08d4f3" />

**Key Metrics:**
- Unique Products: 32,951
- Total Items Sold: 112,650
- Avg Items per Order: 1.13

</details>

---

## 🚀 Installation & Setup

### **Prerequisites**

Ensure you have the following installed:

- **Python 3.14.2** - [Download](https://www.python.org/downloads/)
- **PostgreSQL 18** - [Download](https://www.postgresql.org/download/)
- **Power BI Desktop** (optional) - [Download](https://powerbi.microsoft.com/desktop/)
- **Git** - [Download](https://git-scm.com/downloads)

### **Step 1: Clone Repository**

```bash
git clone https://github.com/yourusername/ecommerce-etl-project.git
cd ecommerce-etl-project
```

### **Step 2: Create Virtual Environment**

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**macOS/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### **Step 3: Install Dependencies**

```bash
pip install -r requirements.txt
```

### **Step 4: Configure Environment Variables**

Create a `.env` file in the project root:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_dw
DB_USER=postgres
DB_PASSWORD=your_password_here

# Optional: Logging Configuration
LOG_LEVEL=INFO
```

**⚠️ Important:** Never commit `.env` to version control (already in `.gitignore`)

### **Step 5: Download Dataset**

1. Download the [Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle
2. Extract all CSV files
3. Place them in the `data/raw/` directory:
   ```
   data/raw/
   ├── olist_customers_dataset.csv
   ├── olist_orders_dataset.csv
   ├── olist_order_items_dataset.csv
   ├── olist_order_payments_dataset.csv
   ├── olist_order_reviews_dataset.csv
   ├── olist_products_dataset.csv
   ├── olist_sellers_dataset.csv
   └── olist_geolocation_dataset.csv
   ```

### **Step 6: Setup PostgreSQL Database**

1. Open **pgAdmin** or **psql**
2. Create database:
   ```sql
   CREATE DATABASE ecommerce_dw;
   ```
3. Verify connection using credentials from `.env`

---

## 🎮 Usage

### **Running the Complete ETL Pipeline**

Execute each phase in sequence:

#### **Phase 1: Extract Data**
```bash
python src/extract/extract_data.py
```
- Validates source CSV files
- Performs quality checks
- Copies files to `data/raw/`
- Logs extraction summary

#### **Phase 2: Transform Data**
```bash
python src/transform/transform_data.py
```
- Transforms raw data into star schema
- Creates dimension tables (date, products, customers, payment_type)
- Builds fact tables (orders, cohort_retention)
- Generates surrogate keys
- Saves transformed data to `data/staging/`

#### **Phase 3: Create Database Schema**
```bash
python src/schema/create_schema.py
```
- Drops existing tables (if any)
- Creates 6 tables with proper constraints
- Establishes primary and foreign keys
- Creates indexes for performance

#### **Phase 4: Load Dimension Tables**
```bash
python src/load/load_dimensions.py
```
- Loads dim_date (1,826 rows)
- Loads dim_products (32,951 rows)
- Loads dim_customers (99,441 rows)
- Loads dim_payment_type (5 rows)
- Validates data integrity

#### **Phase 5: Load Fact Tables**
```bash
python src/load/load_facts.py
```
- Loads fact_orders (99,441 rows) in chunks
- Loads fact_cohort_retention (1,077 rows)
- Validates foreign key relationships
- Displays analytics summary

#### **Data Quality Check**
```bash
python src/load/check_data_quality.py
```
- Verifies row counts
- Checks referential integrity
- Validates NULL values
- Tests business logic
- Generates quality report

### **Running All Phases at Once**

Create a `run_pipeline.bat` (Windows) or `run_pipeline.sh` (macOS/Linux):

**Windows (run_pipeline.bat):**
```batch
@echo off
echo Starting ETL Pipeline...
echo.

echo [1/6] Extracting data...
python src/extract/extract_data.py
if %errorlevel% neq 0 exit /b %errorlevel%

echo [2/6] Transforming data...
python src/transform/transform_data.py
if %errorlevel% neq 0 exit /b %errorlevel%

echo [3/6] Creating database schema...
python src/schema/create_schema.py
if %errorlevel% neq 0 exit /b %errorlevel%

echo [4/6] Loading dimensions...
python src/load/load_dimensions.py
if %errorlevel% neq 0 exit /b %errorlevel%

echo [5/6] Loading facts...
python src/load/load_facts.py
if %errorlevel% neq 0 exit /b %errorlevel%

echo [6/6] Running quality checks...
python src/load/check_data_quality.py

echo.
echo ✓ ETL Pipeline Complete!
pause
```

**macOS/Linux (run_pipeline.sh):**
```bash
#!/bin/bash
set -e

echo "Starting ETL Pipeline..."
echo ""

echo "[1/6] Extracting data..."
python src/extract/extract_data.py

echo "[2/6] Transforming data..."
python src/transform/transform_data.py

echo "[3/6] Creating database schema..."
python src/schema/create_schema.py

echo "[4/6] Loading dimensions..."
python src/load/load_dimensions.py

echo "[5/6] Loading facts..."
python src/load/load_facts.py

echo "[6/6] Running quality checks..."
python src/load/check_data_quality.py

echo ""
echo "✓ ETL Pipeline Complete!"
```

Make executable: `chmod +x run_pipeline.sh`

### **Opening Power BI Dashboard**

1. Open `dashboards/ecommerce_analytics_dashboard.pbix` in Power BI Desktop
2. If data doesn't load:
   - Go to Home → Transform data
   - Update PostgreSQL connection credentials
   - OR run: `python src/export/export_for_powerbi.py` and import CSVs

---

## 💡 Skills Demonstrated

### **Data Engineering**
- ✅ ETL pipeline design and implementation
- ✅ Data extraction from multiple CSV sources
- ✅ Complex data transformations using pandas
- ✅ Incremental and bulk loading strategies
- ✅ Error handling and data validation

### **Database Design**
- ✅ Dimensional modeling (star schema)
- ✅ Surrogate key generation
- ✅ Foreign key relationships and referential integrity
- ✅ SQL DDL and DML operations
- ✅ Database performance optimization

### **Data Quality**
- ✅ Automated data validation
- ✅ NULL value handling
- ✅ Duplicate detection
- ✅ Business logic validation
- ✅ Referential integrity checks

### **Business Intelligence**
- ✅ DAX measure creation
- ✅ Interactive dashboard design
- ✅ Data visualization best practices
- ✅ User experience optimization
- ✅ Insight generation from data

### **Software Engineering**
- ✅ Modular code structure
- ✅ Configuration management (.env)
- ✅ Comprehensive logging
- ✅ Version control (Git)
- ✅ Documentation

### **Analytics**
- ✅ Cohort analysis
- ✅ Customer segmentation
- ✅ Revenue analysis
- ✅ Retention metrics
- ✅ Operational KPIs

---

## 🚧 Challenges & Solutions

### **Challenge 1: Large Dataset Performance**
**Problem:** Loading 99K+ fact records caused memory issues and slow performance

**Solution:**
- Implemented chunked loading (5,000 rows per batch)
- Used pandas `chunksize` parameter in `to_sql()`
- Added progress logging for monitoring
- Result: Reduced load time from 15 minutes to 3 minutes

### **Challenge 2: Foreign Key Mismatches**
**Problem:** Some fact_orders records had customer_key values not present in dim_customers

**Solution:**
- Added pre-load validation to check FK existence
- Implemented error handling to skip orphaned records
- Logged problematic records for investigation
- Result: Achieved 99.8% referential integrity

### **Challenge 3: Power BI Connection Issues**
**Problem:** Power BI couldn't connect directly to PostgreSQL (driver issues)

**Solution:**
- Installed PostgreSQL ODBC driver (64-bit)
- Created System DSN in Windows ODBC Administrator
- Alternative: Created CSV export script for offline import
- Result: Successful Power BI connection with multiple fallback options

### **Challenge 4: Date Sorting in Power BI**
**Problem:** Month names sorted alphabetically (Apr, Aug, Dec...) instead of chronologically

**Solution:**
- Created `month_sort` calculated column using `MONTH()` DAX function
- Set `month_name` column to "Sort by Column" = `month_sort`
- Result: Proper chronological sorting (Jan, Feb, Mar...)

### **Challenge 5: Cohort Retention Calculation**
**Problem:** Complex logic to calculate retention rate by cohort and month since first purchase

**Solution:**
- Created pandas groupby with multiple aggregations
- Used date arithmetic to calculate months_since_first_purchase
- Implemented retention rate formula: (returning customers / cohort size)
- Result: Accurate cohort retention heatmap in dashboard

---

### **Sample SQL Queries**

<details>
<summary>Click to view sample queries</summary>

**Top 10 Customers by Revenue:**
```sql
SELECT 
    c.customer_id,
    c.customer_state,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(f.order_total_value) as lifetime_value
FROM fact_orders f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE f.is_completed_order = TRUE
GROUP BY c.customer_id, c.customer_state
ORDER BY lifetime_value DESC
LIMIT 10;
```

**Monthly Revenue Trend:**
```sql
SELECT 
    d.year,
    d.month_name,
    COUNT(*) as order_count,
    SUM(f.order_total_value) as monthly_revenue,
    AVG(f.order_total_value) as avg_order_value
FROM fact_orders f
JOIN dim_date d ON f.order_date_key = d.date_key
WHERE f.is_completed_order = TRUE
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

**Customer Retention by Cohort:**
```sql
SELECT 
    cohort_month,
    months_since_first_purchase,
    customer_count,
    retention_rate
FROM fact_cohort_retention
WHERE cohort_month >= '2017-01-01'
ORDER BY cohort_month, months_since_first_purchase;
```

**Product Category Performance:**
```sql
SELECT 
    p.product_category_segment,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.order_total_value) as total_revenue,
    AVG(f.order_total_value) as avg_order_value,
    AVG(f.review_score) as avg_review_score
FROM fact_orders f
JOIN dim_products p ON f.product_key = p.product_key
WHERE f.is_completed_order = TRUE
GROUP BY p.product_category_segment
ORDER BY total_revenue DESC;
```

</details>

---

## 📄 License

This project is licensed under the **MIT License**.

---

## 📧 Contact

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/shiu-kong-yu-971556105/)
[![Portfolio](https://img.shields.io/badge/Portfolio-Visit-green?style=for-the-badge&logo=google-chrome)](https://yu-shiu-kong.gitbook.io/data-projects/)
[![Email](https://img.shields.io/badge/Email-Contact-red?style=for-the-badge&logo=gmail)](cedricyu80@gmail.com)

---

## 🙏 Acknowledgments

- **Dataset:** [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) on Kaggle
- **Inspiration:** Real-world e-commerce analytics challenges
- **Tools:** PostgreSQL, Python pandas, Power BI communities

---

<div align="center">

**Built with ❤️ using Python, PostgreSQL, and Power BI**

[⬆ Back to Top](#-e-commerce-etl-data-warehouse--analytics-dashboard)

</div>
