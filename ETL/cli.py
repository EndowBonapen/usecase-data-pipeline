import asyncio
import json
from pathlib import Path

import typer

from extract.inventory import fetch_inventory_batches
from extract.land import land_raw
from extract.relational_seed import fetch_relational_seed_batches
from extract.returns import fetch_returns_batches
from extract.transactions import fetch_transactions_batches
from transform.clean import cast_money_columns, validate
from transform.flatten import FLATTENERS

app = typer.Typer(help="E-commerce data platform ETL CLI")
extract_app = typer.Typer(help="Extract raw data from randomapi.dev")
transform_app = typer.Typer(help="Transform landed raw data")
app.add_typer(extract_app, name="extract")
app.add_typer(transform_app, name="transform")


def _land_and_report(source: str, results: list[dict]) -> None:
    for batch_index, payload in enumerate(results, start=1):
        typer.echo(f"--- batch {batch_index} ---")
        rows = payload["data"]
        if isinstance(rows, dict) and "tables" in rows:
            for table_name, table_rows in rows["tables"].items():
                typer.echo(f"{table_name}: {len(table_rows)} rows")
        else:
            typer.echo(f"{len(rows)} rows")

        file_path = land_raw(source, batch_index, payload)
        typer.echo(f"landed -> {file_path}")


@extract_app.command("relational-seed")
def extract_relational_seed(
    schema: str = "ecommerce",
    customers: int = 20,
    orders_per_customer: int = 3,
    items_per_order: int = 3,
    id_style: str = "uuid",
    currency: str = "USD",
    batches: int = 1,
    seed_start: int | None = None,
) -> None:
    """Fetch the master spine (categories, products, customers, orders, order_items).

    Use --batches to fetch more than one call's worth — each single call is
    capped at 400 rows total across all 5 tables (see relational_seed.py).
    """
    results = asyncio.run(
        fetch_relational_seed_batches(
            batches=batches,
            schema=schema,
            customers=customers,
            orders_per_customer=orders_per_customer,
            items_per_order=items_per_order,
            id_style=id_style,
            currency=currency,
            seed_start=seed_start,
        )
    )
    _land_and_report("relational_seed", results)


@extract_app.command("transactions")
def extract_transactions(
    count: int = 100,
    min_amount: float = None,
    max_amount: float = None,
    currency: str = "USD",
    date_from: str = None,
    date_to: str = None,
    batches: int = 1,
) -> None:
    """Fetch payment/financial transaction events."""
    results = asyncio.run(
        fetch_transactions_batches(
            batches=batches,
            count=count,
            min_amount=min_amount,
            max_amount=max_amount,
            currency=currency,
            date_from=date_from,
            date_to=date_to,
        )
    )
    _land_and_report("transactions", results)


@extract_app.command("inventory")
def extract_inventory(
    count: int = 100,
    min_available: int = None,
    location_count: int = None,
    include_incoming: bool = None,
    as_of: str = None,
    batches: int = 1,
) -> None:
    """Fetch stock snapshot per SKU per warehouse."""
    results = asyncio.run(
        fetch_inventory_batches(
            batches=batches,
            count=count,
            min_available=min_available,
            location_count=location_count,
            include_incoming=include_incoming,
            as_of=as_of,
        )
    )
    _land_and_report("inventory", results)


@extract_app.command("returns")
def extract_returns(
    count: int = 100,
    currency: str = "USD",
    date_from: str = None,
    date_to: str = None,
    min_items: int = None,
    max_items: int = None,
    batches: int = 1,
) -> None:
    """Fetch return/RMA lifecycle records."""
    results = asyncio.run(
        fetch_returns_batches(
            batches=batches,
            count=count,
            currency=currency,
            date_from=date_from,
            date_to=date_to,
            min_items=min_items,
            max_items=max_items,
        )
    )
    _land_and_report("returns", results)


@transform_app.command("flatten")
def transform_flatten(file: Path) -> None:
    """Flatten a landed raw JSON file (from `extract ...`) into Arrow tables."""
    envelope = json.loads(file.read_text(encoding="utf-8"))
    source = envelope["source"]

    flattener = FLATTENERS.get(source)
    if flattener is None:
        raise typer.BadParameter(f"no flattener registered for source '{source}'")

    tables = flattener(envelope["payload"])
    for table_name, table in tables.items():
        typer.echo(f"{table_name}: {table.num_rows} rows, columns={table.column_names}")


@transform_app.command("clean")
def transform_clean(file: Path) -> None:
    """Validate derived fields, then cast money columns (_minor -> _amount)."""
    envelope = json.loads(file.read_text(encoding="utf-8"))
    source = envelope["source"]

    flattener = FLATTENERS.get(source)
    if flattener is None:
        raise typer.BadParameter(f"no flattener registered for source '{source}'")

    tables = flattener(envelope["payload"])

    violations = validate(source, tables)
    if violations:
        typer.echo(f"Found {len(violations)} validation issue(s):")
        for violation in violations:
            typer.echo(f"  {violation}")
    else:
        typer.echo("All derived-field checks passed.")

    for table_name, table in tables.items():
        cleaned = cast_money_columns(table)
        typer.echo(f"{table_name}: {cleaned.num_rows} rows, columns={cleaned.column_names}")


if __name__ == "__main__":
    app()
