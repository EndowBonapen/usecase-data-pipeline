import pyarrow as pa


def flatten_dict(d: dict, parent_key: str = "") -> dict:
    """Flatten nested dict values into prefixed keys. Lists are left untouched —
    arrays get exploded into their own child table instead (see the per-source
    functions below), not inlined as flat columns."""
    items = {}
    for key, value in d.items():
        new_key = f"{parent_key}_{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key))
        else:
            items[new_key] = value
    return items


def _to_table(rows: list[dict]) -> pa.Table | None:
    return pa.Table.from_pylist(rows) if rows else None


def _tables(**named_rows: list[dict]) -> dict[str, pa.Table]:
    return {name: table for name, rows in named_rows.items() if (table := _to_table(rows)) is not None}


def flatten_relational_seed(payload: dict) -> dict[str, pa.Table]:
    """categories/products/customers/orders/order_items are already flat rows."""
    tables = payload["data"]["tables"]
    return _tables(**tables)


def flatten_transactions(payload: dict) -> dict[str, pa.Table]:
    """Already flat rows — no nested objects or arrays."""
    return _tables(transactions=payload["data"])


def flatten_inventory(payload: dict) -> dict[str, pa.Table]:
    """Nested product{}/warehouse{} get flattened inline; locations[]/incoming[]
    become their own child tables keyed by the parent inventory record's id."""
    inventory_rows = []
    location_rows = []
    incoming_rows = []

    for row in payload["data"]:
        inventory_id = row["id"]
        flat_source = {k: v for k, v in row.items() if k not in ("locations", "incoming")}
        inventory_rows.append(flatten_dict(flat_source))

        for location in row.get("locations") or []:
            location_rows.append({"inventory_id": inventory_id, **location})

        for incoming in row.get("incoming") or []:
            incoming_rows.append({"inventory_id": inventory_id, **incoming})

    return _tables(
        inventory=inventory_rows,
        inventory_locations=location_rows,
        inventory_incoming=incoming_rows,
    )


def flatten_returns(payload: dict) -> dict[str, pa.Table]:
    """Nested refund{}/reverseShipment{} get flattened inline; items[]/events[]
    become their own child tables keyed by the parent return record's id.

    refund/reverseShipment can be null on some records — normalized to {} first
    so every row produces the same flattened key set (a row with a null nested
    object and a row with a populated one must not end up with different
    columns, or pa.Table.from_pylist can't build one consistent schema).
    """
    return_rows = []
    item_rows = []
    event_rows = []

    for row in payload["data"]:
        return_id = row["id"]
        normalized = dict(row)
        normalized["refund"] = normalized.get("refund") or {}
        normalized["reverseShipment"] = normalized.get("reverseShipment") or {}
        flat_source = {k: v for k, v in normalized.items() if k not in ("items", "events")}
        return_rows.append(flatten_dict(flat_source))

        for item in row.get("items") or []:
            item_rows.append({"return_id": return_id, **item})

        for event in row.get("events") or []:
            event_rows.append({"return_id": return_id, **event})

    return _tables(
        returns=return_rows,
        return_items=item_rows,
        return_events=event_rows,
    )


FLATTENERS = {
    "relational_seed": flatten_relational_seed,
    "transactions": flatten_transactions,
    "inventory": flatten_inventory,
    "returns": flatten_returns,
}
