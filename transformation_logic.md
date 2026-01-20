# Data Transformation Logic

## Business Rules

### Customer Segmentation
- **New Customer**: total_orders = 1
- **Returning Customer**: 2 ≤ total_orders ≤ 5
- **VIP Customer**: total_orders > 5 OR lifetime_value > R$ 5000

### Regional Mapping (Brazilian States)
- **Southeast**: SP, RJ, MG, ES
- **South**: PR, SC, RS
- **Northeast**: BA, CE, PE, RN, PB, AL, SE, MA, PI
- **North**: AM, PA, AC, RO, RR, AP, TO
- **Central-West**: GO, MT, MS, DF

### Product Category Grouping
- **Electronics**: computadores_acessorios, telefonia, eletrônicos, etc.
- **Home & Garden**: casa_conforto, cama_mesa_banho, ferramentas_jardim, etc.
- **Fashion**: moda_esportiva, relogios_presentes, fashion_bolsas_e_acessorios
- **Health & Beauty**: beleza_saude, perfumaria
- **Other**: All remaining categories

### Date Dimension Generation
- Start Date: 2016-09-01 (earliest order date from EDA)
- End Date: 2018-10-31 (latest order date + buffer)
- Brazilian Holidays to flag: New Year, Carnival, Easter, Independence Day, Christmas

### CLV Calculation
CLV = (Average Order Value) × (Purchase Frequency) × (Customer Lifespan Estimate)

Where:
- Average Order Value = Total revenue / Total orders per customer
- Purchase Frequency = Total orders / Days since first purchase (annualized)
- Customer Lifespan Estimate = 365 days (assumption)

### Delivery Performance
- **On-Time:** delivered_date ≤ estimated_delivery_date
- **Late:** delivered_date > estimated_delivery_date
- **Very Late:** delivery_delay_days > 7
- **Not Delivered:** order_status ≠ 'delivered'

## Data Quality Rules
### Missing Value Handling

- **delivered_date IS NULL:**

  - Filter out if order_status = 'canceled' or 'unavailable'
  - Flag as 'in_transit' if order_status = 'shipped'

- **review_score IS NULL:**

  - Set has_review = FALSE
  - Keep review_score as NULL (not all orders have reviews)

- **product_category_name IS NULL:**

   - Replace with 'Uncategorized' category

### Outlier Handling

- **Price outliers:**

  - Flag items with price > 99th percentile for review
  - Keep in dataset but mark with is_outlier flag

- **Delivery time outliers:**

  - delivery_days > 90: Flag for investigation
  - delivery_days < 0: Data error - exclude from analysis

### Duplicate Handling

- **customer_unique_id duplicates:** Expected (same person, multiple orders)
- **order_id duplicates:** Should NOT exist - data error if found
- **Multiple payments per order:** Sum payment_value, take primary payment_type

## Aggregation Logic
### From Order Items to Order Level

**For each order_id:**
  - order_item_count = COUNT(*)
  - order_subtotal = SUM(price)
  - order_freight_total = SUM(freight_value)
  - order_total_value = order_subtotal + order_freight_total
  - Keep first product_id (or most expensive) if single row needed

### Payment Aggregation

**For each order_id:**
  - payment_value = SUM(payment_value) across all payment records
  - payment_type = payment_type with MAX(payment_value)
  - payment_installments = MAX(payment_installments)


