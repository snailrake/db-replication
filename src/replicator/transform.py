from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any


def _as_float(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


def build_customer_documents(rows: list[dict[str, Any]], synced_at: datetime) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    customers_map: dict[int, dict[str, Any]] = {}
    orders_map: dict[tuple[int, int], dict[str, Any]] = {}

    for row in rows:
        customer_id = row["customer_id"]
        customer_doc = customers_map.get(customer_id)
        if customer_doc is None:
            customer_doc = {
                "_id": customer_id,
                "name": row["customer_name"],
                "email": row["customer_email"],
                "created_at": row["customer_created_at"],
                "updated_at": row["customer_updated_at"],
                "deleted_at": row["customer_deleted_at"],
                "synced_at": synced_at,
                "orders": [],
            }
            customers_map[customer_id] = customer_doc
            documents.append(customer_doc)

        order_id = row["order_id"]
        if order_id is None:
            continue

        order_key = (customer_id, order_id)
        order_doc = orders_map.get(order_key)
        if order_doc is None:
            order_doc = {
                "order_id": order_id,
                "product": row["order_product"],
                "amount": _as_float(row["order_amount"]),
                "status": row["order_status"],
                "placed_at": row["order_created_at"],
                "updated_at": row["order_updated_at"],
                "deleted_at": row["order_deleted_at"],
                "products": [],
            }
            orders_map[order_key] = order_doc
            customer_doc["orders"].append(order_doc)

        if row["product_id"] is None:
            continue

        product_doc = {
            "product_id": row["product_id"],
            "name": row["product_name"],
            "sku": row["product_sku"],
            "price": _as_float(row["product_price"]),
            "created_at": row["product_created_at"],
            "updated_at": row["product_updated_at"],
            "deleted_at": row["product_deleted_at"],
        }
        order_doc["products"].append(product_doc)

    return documents
