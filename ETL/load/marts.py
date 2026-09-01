from google.cloud import bigquery

from config import BQ_DATASET_MARTS, BQ_DATASET_RAW, GCP_PROJECT_ID

RAW = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}"
MARTS = f"{GCP_PROJECT_ID}.{BQ_DATASET_MARTS}"

# Scoped to relational_seed only — its FKs are guaranteed by the API itself
# (plan-project.md §3). transactions/inventory/returns need identity
# crosswalk first, which is a Phase 2 (dbt) job, not this.
MART_STATEMENTS = {
    "dim_category": f"""
        CREATE OR REPLACE TABLE `{MARTS}.dim_category` AS
        SELECT
            id AS category_id,
            slug,
            name
        FROM `{RAW}.categories`
    """,
    "dim_product": f"""
        CREATE OR REPLACE TABLE `{MARTS}.dim_product` AS
        SELECT
            id AS product_id,
            category_id,
            sku,
            name,
            currency,
            in_stock,
            created_at,
            price_amount
        FROM `{RAW}.products`
    """,
    "dim_customer": f"""
        CREATE OR REPLACE TABLE `{MARTS}.dim_customer` AS
        SELECT
            id AS customer_id,
            email,
            full_name,
            city,
            country_code,
            is_active,
            created_at
        FROM `{RAW}.customers`
    """,
    "fact_orders": f"""
        CREATE OR REPLACE TABLE `{MARTS}.fact_orders` AS
        SELECT
            id AS order_id,
            customer_id,
            order_number,
            status,
            currency,
            placed_at,
            shipped_at,
            total_amount
        FROM `{RAW}.orders`
    """,
    "fact_order_items": f"""
        CREATE OR REPLACE TABLE `{MARTS}.fact_order_items` AS
        SELECT
            id AS order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price_amount,
            line_total_amount
        FROM `{RAW}.order_items`
    """,
}

MART_BUILD_ORDER = ["dim_category", "dim_product", "dim_customer", "fact_orders", "fact_order_items"]


def build_marts(client: bigquery.Client) -> dict[str, int]:
    """Run each CREATE OR REPLACE TABLE statement, return row count per mart."""
    row_counts = {}
    for name in MART_BUILD_ORDER:
        client.query(MART_STATEMENTS[name]).result()
        row_counts[name] = client.get_table(f"{MARTS}.{name}").num_rows
    return row_counts
