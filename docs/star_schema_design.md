# Star Schema Design - Olist E-Commerce Data Warehouse

## Overview
This data warehouse uses a **star schema** design optimized for answering two primary business questions:
1. How can we increase repeat purchases and identify our most valuable customer segments?
2. What factors impact our delivery performance and profitability?

## Schema Diagram

                ┌─────────────────┐
                │   DIM_DATE      │
                ├─────────────────┤
                │ date_key (PK)   │
                │ full_date       │
                │ year            │
                │ quarter         │
                │ month           │
                │ is_weekend      │
                └─────────────────┘
                        ▲
                        │
┌─────────────────┐ │ ┌─────────────────┐
│ DIM_CUSTOMERS │ │ │ DIM_PRODUCTS │
├─────────────────┤ │ ├─────────────────┤
│ customer_key(PK)│ │ │ product_key(PK) │
│ customer_id │ │ │ product_id │
│ state │ │ │ category │
│ region │ │ │ weight_g │
│ segment │◄────────┼────────►│ has_photos │
│ total_orders │ │ └─────────────────┘
│ lifetime_value │ │
└─────────────────┘ │
│
┌───────▼─────────┐
│ FACT_ORDERS │
├─────────────────┤
│ order_key (PK) │
│ customer_key(FK)│
│ product_key(FK) │
│ order_date_key │
│ payment_type_key│
├─────────────────┤
│ order_total │
│ freight_total │
│ delivery_days │
│ is_late │
│ review_score │
└─────────────────┘
▲
│
┌───────┴─────────┐
│ DIM_PAYMENT_TYPE│
├─────────────────┤
│ payment_type_key│
│ payment_type │
│ payment_category│
└─────────────────┘


## Dimension Tables

### DIM_CUSTOMERS
**Purpose:** Customer segmentation and lifetime value analysis

| Column Name | Data Type | Description |
|------------|-----------|-------------|
| customer_key | INT (PK) | Surrogate key |
| customer_unique_id | VARCHAR | Business key from source |
| customer_city | VARCHAR | City name |
| customer_state | VARCHAR(2) | State code |
| customer_region | VARCHAR | Derived: North, South, Southeast, etc. |
| customer_segment | VARCHAR | New/Returning/VIP based on order count |
| first_order_date | DATE | Date of first purchase |
| last_order_date | DATE | Date of most recent purchase |
| total_orders | INT | Lifetime order count |
| lifetime_value | DECIMAL | Total revenue from customer (CLV) |

**Source:** `olist_customers_dataset.csv` + aggregated from orders

---

### DIM_PRODUCTS
**Purpose:** Product category analysis and revenue breakdown

| Column Name | Data Type | Description |
|------------|-----------|-------------|
| product_key | INT (PK) | Surrogate key |
| product_id | VARCHAR | Business key from source |
| product_category_english | VARCHAR | Translated category name |
| product_category_segment | VARCHAR | Grouped: Electronics, Home, Fashion |
| product_weight_g | INT | Weight in grams |
| product_volume_cm3 | INT | Calculated volume (L×W×H) |
| has_photos | BOOLEAN | Product has photos |

**Source:** `olist_products_dataset.csv`

---

### DIM_DATE
**Purpose:** Time-series analysis and trend detection

| Column Name | Data Type | Description |
|------------|-----------|-------------|
| date_key | INT (PK) | YYYYMMDD format |
| full_date | DATE | Actual date |
| year, quarter, month | INT | Time period components |
| day_of_week | INT | 0=Sunday, 6=Saturday |
| is_weekend | BOOLEAN | Weekend flag |
| is_holiday | BOOLEAN | Brazilian holiday flag |

**Source:** Generated programmatically (2016-09-01 to 2018-10-31)

---

### DIM_PAYMENT_TYPE
**Purpose:** Payment method analysis

| Column Name | Data Type | Description |
|------------|-----------|-------------|
| payment_type_key | INT (PK) | Surrogate key |
| payment_type | VARCHAR | credit_card, boleto, debit_card, voucher |
| payment_category | VARCHAR | Credit, Cash, Digital |

**Source:** Distinct values from `olist_order_payments_dataset.csv`

---

## Fact Table

### FACT_ORDERS
**Grain:** One row per order (aggregated from order items)

**Purpose:** Central metrics for revenue, delivery, and customer behavior analysis

#### Foreign Keys
- customer_key → DIM_CUSTOMERS
- product_key → DIM_PRODUCTS (primary product)
- order_date_key → DIM_DATE
- delivery_date_key → DIM_DATE
- payment_type_key → DIM_PAYMENT_TYPE

#### Measures
| Column Name | Data Type | Description |
|------------|-----------|-------------|
| order_item_count | INT | Number of products in order |
| order_subtotal | DECIMAL | Sum of item prices |
| order_freight_total | DECIMAL | Total shipping cost |
| order_total_value | DECIMAL | Subtotal + Freight |
| payment_value | DECIMAL | Amount paid |
| payment_installments | INT | Number of installments |
| delivery_days | INT | Days from purchase to delivery |
| delivery_delay_days | INT | Actual - Estimated (+ = late) |
| is_late_delivery | BOOLEAN | Delivery past estimated date |
| review_score | INT | 1-5 stars (NULL if no review) |

**Sources:** 
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`

---

## Design Decisions

### Why Star Schema?
- **Simplicity:** Easy to understand and query for business users
- **Performance:** Fewer joins than normalized schemas
- **Flexibility:** Easy to add new dimensions without restructuring

### Why Order-Level Grain (Not Line-Item)?
- Business questions focus on order-level metrics (AOV, delivery time)
- Simpler for dashboard users to understand
- Better query performance for executive reports
- Line-item detail preserved in source if needed for deep dives

### Key Transformations
1. **Customer Segmentation:** Calculated based on order frequency
2. **Regional Grouping:** Brazilian states mapped to 5 regions
3. **Product Categorization:** 70+ categories grouped into 5 segments
4. **CLV Calculation:** Lifetime value computed from historical orders
5. **Delivery Metrics:** Business-friendly delivery performance KPIs

---

## Data Lineage

**Extract:**
- Read 6 CSV files with data validation

**Transform:**
- Create surrogate keys
- Apply business rules (segmentation, grouping)
- Calculate derived metrics (CLV, delivery delays)
- Handle missing values and outliers

**Load:**
- Populate dimension tables first
- Load fact table with foreign key references
- Create indexes for query optimization
