from decimal import Decimal

import pyarrow as pa

MONEY_SUFFIXES = ("_minor", "Minor")
MONEY_DECIMAL_TYPE = pa.decimal128(24, 2)


def cast_money_columns(table: pa.Table) -> pa.Table:
    """Convert integer minor-unit columns (cents) into exact decimal amount
    columns. Done via Python's Decimal, not float division — Arrow's decimal
    compute functions ran into precision/scale issues, and float division
    reintroduces the binary rounding error minor-unit integers exist to avoid.
    """
    for name in list(table.column_names):
        if name.endswith("_minor"):
            new_name = name[: -len("_minor")] + "_amount"
        elif name.endswith("Minor"):
            new_name = name[: -len("Minor")] + "_amount"
        else:
            continue

        decimals = [Decimal(v) / 100 if v is not None else None for v in table[name].to_pylist()]
        amount_column = pa.array(decimals, type=MONEY_DECIMAL_TYPE)

        index = table.column_names.index(name)
        table = table.remove_column(index)
        table = table.append_column(new_name, amount_column)

    return table


def validate_order_item_totals(order_items: pa.Table) -> list[dict]:
    """line_total_minor must equal quantity * unit_price_minor."""
    violations = []
    for row in order_items.to_pylist():
        expected = row["quantity"] * row["unit_price_minor"]
        if expected != row["line_total_minor"]:
            violations.append(
                {
                    "check": "order_item_line_total",
                    "order_item_id": row["id"],
                    "expected": expected,
                    "actual": row["line_total_minor"],
                }
            )
    return violations


def validate_order_totals(orders: pa.Table, order_items: pa.Table) -> list[dict]:
    """orders.total_minor must equal the sum of its order_items.line_total_minor."""
    sum_by_order_id: dict[str, int] = {}
    for row in order_items.to_pylist():
        sum_by_order_id[row["order_id"]] = sum_by_order_id.get(row["order_id"], 0) + row["line_total_minor"]

    violations = []
    for row in orders.to_pylist():
        expected = sum_by_order_id.get(row["id"], 0)
        if expected != row["total_minor"]:
            violations.append(
                {
                    "check": "order_total",
                    "order_id": row["id"],
                    "expected": expected,
                    "actual": row["total_minor"],
                }
            )
    return violations


def validate_inventory_available(inventory: pa.Table) -> list[dict]:
    """available must equal onHand - reserved (the API's own documented formula)."""
    violations = []
    for row in inventory.to_pylist():
        expected = row["onHand"] - row["reserved"]
        if expected != row["available"]:
            violations.append(
                {
                    "check": "inventory_available",
                    "inventory_id": row["id"],
                    "expected": expected,
                    "actual": row["available"],
                }
            )
    return violations


def validate_return_refund_estimate(returns: pa.Table) -> list[dict]:
    """refund_estimatedMinor must equal refund_subtotalMinor - refund_restockingFeeMinor."""
    violations = []
    for row in returns.to_pylist():
        expected = row["refund_subtotalMinor"] - row["refund_restockingFeeMinor"]
        if expected != row["refund_estimatedMinor"]:
            violations.append(
                {
                    "check": "return_refund_estimate",
                    "return_id": row["id"],
                    "expected": expected,
                    "actual": row["refund_estimatedMinor"],
                }
            )
    return violations


def validate(source: str, tables: dict[str, pa.Table]) -> list[dict]:
    violations = []

    if source == "relational_seed":
        if "order_items" in tables:
            violations += validate_order_item_totals(tables["order_items"])
        if "orders" in tables and "order_items" in tables:
            violations += validate_order_totals(tables["orders"], tables["order_items"])
    elif source == "inventory":
        if "inventory" in tables:
            violations += validate_inventory_available(tables["inventory"])
    elif source == "returns":
        if "returns" in tables:
            violations += validate_return_refund_estimate(tables["returns"])

    return violations
