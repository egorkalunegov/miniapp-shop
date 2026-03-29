import os
import json
import hmac
import hashlib
import sqlite3
import uuid
import re
import asyncio
import time
import shutil
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

import httpx
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Request, HTTPException, Response, UploadFile, File, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
import secrets

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

def _env_str(name: str, default: str = "") -> str:
    val = os.getenv(name, default)
    if val is None:
        return default
    val = str(val).strip()
    if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
        val = val[1:-1].strip()
    return val


PRODAMUS_FORM_URL = _env_str("PRODAMUS_FORM_URL", "")
PRODAMUS_SYS = _env_str("PRODAMUS_SYS", "")
PRODAMUS_SECRET_KEY = _env_str("PRODAMUS_SECRET_KEY", "")
PRODAMUS_SIGN_MODE = _env_str("PRODAMUS_SIGN_MODE", "ascii").lower()
PRODAMUS_SIGN_SOURCE = _env_str("PRODAMUS_SIGN_SOURCE", "flat").lower()
PRODAMUS_MINIMAL = _env_str("PRODAMUS_MINIMAL", "0").lower() in ("1", "true", "yes")
PRODAMUS_AMOUNT_ONLY = _env_str("PRODAMUS_AMOUNT_ONLY", "0").lower() in ("1", "true", "yes")
PRODAMUS_NO_ORDER_ID = _env_str("PRODAMUS_NO_ORDER_ID", "0").lower() in ("1", "true", "yes")
PRODAMUS_DIRECT_ONLY = _env_str("PRODAMUS_DIRECT_ONLY", "0").lower() in ("1", "true", "yes")
PRODAMUS_AUTO_SIGN = _env_str("PRODAMUS_AUTO_SIGN", "0").lower() in ("1", "true", "yes")
PRODAMUS_AUTO_SIGN_TIMEOUT = float(_env_str("PRODAMUS_AUTO_SIGN_TIMEOUT", "6"))
PRODAMUS_SINGLE_PRODUCT_NAME = _env_str("PRODAMUS_SINGLE_PRODUCT_NAME", "Оплата заказа")
PRODAMUS_INCLUDE_EXTRA = _env_str("PRODAMUS_INCLUDE_EXTRA", "0").lower() in ("1", "true", "yes")
PRODAMUS_PHONE_DIGITS = _env_str("PRODAMUS_PHONE_DIGITS", "1").lower() in ("1", "true", "yes")
ADMIN_USER = os.getenv("ADMIN_USER", "").strip()
ADMIN_PASS = os.getenv("ADMIN_PASS", "").strip()
LEADTEH_API_TOKEN = os.getenv("LEADTEH_API_TOKEN", "").strip()
LEADTEH_BOT_ID = os.getenv("LEADTEH_BOT_ID", "").strip()
LEADTEH_PRODUCTS_SCHEMA_ID = os.getenv("LEADTEH_PRODUCTS_SCHEMA_ID", "").strip()
PUBLIC_BASE_URL = _env_str("PUBLIC_BASE_URL", "")
MOYSKLAD_API_BASE = _env_str("MOYSKLAD_API_BASE", "https://api.moysklad.ru/api/remap/1.2").rstrip("/")
MOYSKLAD_TOKEN = _env_str("MOYSKLAD_TOKEN", "")
MOYSKLAD_ORGANIZATION_HREF = _env_str("MOYSKLAD_ORGANIZATION_HREF", "")
MOYSKLAD_STORE_HREF = _env_str("MOYSKLAD_STORE_HREF", "")
MOYSKLAD_ATTR_WEIGHT = _env_str("MOYSKLAD_ATTR_WEIGHT", "Вес")
MOYSKLAD_ATTR_SHELF_LIFE = _env_str("MOYSKLAD_ATTR_SHELF_LIFE", "Срок годности")
MOYSKLAD_ATTR_BADGE = _env_str("MOYSKLAD_ATTR_BADGE", "Бейдж")
MOYSKLAD_ATTR_SORT = _env_str("MOYSKLAD_ATTR_SORT", "Порядок")
MOYSKLAD_ATTR_ACTIVE = _env_str("MOYSKLAD_ATTR_ACTIVE", "Активен")
MOYSKLAD_ATTR_IMAGE_URL = _env_str("MOYSKLAD_ATTR_IMAGE_URL", "URL изображения")

if PRODAMUS_FORM_URL and not PRODAMUS_FORM_URL.endswith("/"):
    PRODAMUS_FORM_URL += "/"

DB_PATH = os.path.join(os.path.dirname(__file__), "app.db")
UPLOADS_DIR = os.path.join(os.path.dirname(__file__), "uploads")
PRODUCT_UPLOADS_DIR = os.path.join(UPLOADS_DIR, "products")

FIXED_PRODUCT_SORTS = {
    "GIFT_BOX_LOVED": 1,
    "DUBAI_CHOCO": 2,
    "ASSORTI_HEART": 3,
    "CHOCO_ORESHEK": 4,
    "CHOCO_ORESHEK_CARAMEL_COOKIE": 5,
    "CHOCO_EGGS_COOKIE": 6,
    "KARTOSHKA": 7,
    "MOTI_Coockies": 8,
    "CHOCO_RASPBERRY_NUTS": 9,
    "CHOCO_RASPBERRY_NUTS_JESUS": 10,
}

LOCAL_CATALOG_OVERRIDE_SKUS = {
    "GIFT_BOX_LOVED",
}


def _fixed_sort_for_sku(sku: str, fallback: int = 0) -> int:
    return FIXED_PRODUCT_SORTS.get((sku or "").strip(), fallback)


def _preserve_local_catalog_fields(sku: str) -> bool:
    return (sku or "").strip() in LOCAL_CATALOG_OVERRIDE_SKUS


def _catalog_override_enabled(row: Optional[sqlite3.Row], sku: str) -> bool:
    if row and int(row["catalog_override"] or 0) == 1:
        return True
    return _preserve_local_catalog_fields(sku)

# ---- сиды каталога (пока Leadteh не настроен) ----
SEED_PRODUCTS = [
    {
        "sku": "GIFT_BOX_LOVED",
        "name": "Подарочный бокс «Это любят Люди»",
        "price": 3990,
        "weight": "770 гр.",
        "shelf_life": "",
        "badge": "ХИТ продаж!",
        "image_url": "/products/gift-box-new.jpeg",
        "description": (
            "1. Пирожное «Картошка» — 1 шт. 0,350 гр\n"
            "2. Дубайский шоколад — 1 шт. 0,180 гр\n"
            "3. Конфеты «Орешек» — 1 шт. 0,120 гр\n"
            "В подарок !!!\n"
            "- Конфеты «В самое сердце» — 1 шт. 0,120 гр"
        ),
        "sort": 1,
        "active": 1,
    },
    {
        "sku": "DUBAI_CHOCO",
        "name": "ДУБАЙСКИЙ ШОКОЛАД",
        "price": 1600,
        "weight": "180 г",
        "shelf_life": "30 суток",
        "badge": "",
        "image_url": "/products/dubai-chocolate.jpg",
        "description": (
            "Состав: Молочный бельгийский шоколад Callebaut, фисташковая паста 100%, "
            "тесто катаифи Bontier, масло сливочное 82,5%."
        ),
        "sort": 2,
        "active": 1,
    },
    {
        "sku": "ASSORTI_HEART",
        "name": "Шоколад конфеты ассорти «В самое сердце»",
        "price": 1000,
        "weight": "120 г",
        "shelf_life": "90 суток",
        "badge": "",
        "image_url": "/products/serdce.jpg",
        "description": (
            "Молочный, белый, апельсиновый, темный бельгийский шоколад Callebaut, "
            "сублимированная малина, орех (миндаль, кешью)."
        ),
        "sort": 3,
        "active": 1,
    },
    {
        "sku": "CHOCO_ORESHEK",
        "name": "Шоколад конфеты «Орешек»",
        "price": 1100,
        "weight": "120 г",
        "shelf_life": "90 суток",
        "badge": "",
        "image_url": "/products/oreshek.jpg",
        "description": (
            "Карамельный бельгийский шоколад Callebaut, взрывная карамель, "
            "орех (миндаль, кешью)."
        ),
        "sort": 4,
        "active": 1,
    },
    {
        "sku": "KARTOSHKA",
        "name": "Пирожное «Картошка»",
        "price": 1000,
        "weight": "350 г",
        "shelf_life": "10 суток",
        "badge": "",
        "image_url": "/products/kartoshka.jpg",
        "description": "Состав: Молочный бельгийский шоколад Callebaut, шоколадный бисквит.",
        "sort": 5,
        "active": 1,
    },
    {
        "sku": "CHOCO_RASPBERRY_NUTS",
        "name": "Шоколад с орехом и малиной",
        "price": 600,
        "weight": "120 г",
        "shelf_life": "120 суток",
        "badge": "",
        "image_url": "/products/malina.jpg",
        "description": "Молочный бельгийский шоколад Callebaut.\nОрех: миндаль, кешью.\nСублимированная малина.",
        "sort": 6,
        "active": 1,
    },
    {
        "sku": "CHOCO_ORESHEK_CARAMEL_COOKIE",
        "name": "Шоколад конфеты «Орешек с карамелью и миндальным печеньем»",
        "price": 1300,
        "weight": "130 г",
        "shelf_life": "90 суток",
        "badge": "",
        "image_url": "/products/caramel.jpg",
        "description": "Молочный бельгийский шоколад Callebaut, карамель, миндальное печенье.",
        "sort": 7,
        "active": 1,
    },
    {
        "sku": "CHOCO_ORESHEK_WALNUT",
        "name": "Конфеты «Орешек с грецким орехом»",
        "price": 1100,
        "weight": "120 г",
        "shelf_life": "3 месяца",
        "badge": "",
        "image_url": "/products/gretcrkiy.jpeg",
        "description": "Состав: Бельгийский молочный шоколад, королевский изюм, грецкий орех.",
        "sort": 8,
        "active": 1,
    },
]


# ---------------------------
# DB
# ---------------------------
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    os.makedirs(PRODUCT_UPLOADS_DIR, exist_ok=True)
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
          order_id TEXT PRIMARY KEY,
          status TEXT NOT NULL,
          amount INTEGER NOT NULL,
          payload_json TEXT NOT NULL,
          payment_url TEXT,
          created_at TEXT DEFAULT (datetime('now')),
          updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    # --- склад ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS inventory (
      sku TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      stock INTEGER NOT NULL,
      reserved INTEGER NOT NULL DEFAULT 0
    )
    """)

    # --- резервы на 30 минут ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reservations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id TEXT UNIQUE NOT NULL,
      status TEXT NOT NULL,              -- active | paid | released | expired
      expires_at TEXT NOT NULL,
      created_at TEXT DEFAULT (datetime('now'))
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reservation_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id TEXT NOT NULL,
      sku TEXT NOT NULL,
      qty INTEGER NOT NULL
    )
    """)
    con.commit()
    con.close()
    ensure_orders_columns()
    ensure_inventory_columns()
    seed_products(insert_only=True)
    return

def ensure_orders_columns() -> None:
    con = db()
    cur = con.cursor()
    cols = {r["name"] for r in cur.execute("PRAGMA table_info(orders)")}

    def add_col(name: str, col_type: str, default_sql: str) -> None:
        if name in cols:
            return
        cur.execute(f"ALTER TABLE orders ADD COLUMN {name} {col_type} DEFAULT {default_sql}")

    add_col("moysklad_demand_href", "TEXT", "''")
    add_col("moysklad_sync_status", "TEXT", "''")
    add_col("moysklad_sync_error", "TEXT", "''")
    add_col("moysklad_synced_at", "TEXT", "NULL")

    con.commit()
    con.close()


def ensure_inventory_columns() -> None:
    con = db()
    cur = con.cursor()
    cols = {r["name"] for r in cur.execute("PRAGMA table_info(inventory)")}

    def add_col(name: str, col_type: str, default_sql: str) -> None:
        if name in cols:
            return
        cur.execute(f"ALTER TABLE inventory ADD COLUMN {name} {col_type} DEFAULT {default_sql}")

    add_col("price", "INTEGER", "0")
    add_col("weight", "TEXT", "''")
    add_col("shelf_life", "TEXT", "''")
    add_col("description", "TEXT", "''")
    add_col("image_url", "TEXT", "''")
    add_col("badge", "TEXT", "''")
    add_col("sort", "INTEGER", "0")
    add_col("active", "INTEGER", "1")
    add_col("catalog_override", "INTEGER", "1")
    add_col("moysklad_href", "TEXT", "''")
    add_col("moysklad_image_href", "TEXT", "''")

    con.commit()
    con.close()


def seed_products(insert_only: bool = True) -> None:
    con = db()
    cur = con.cursor()
    for p in SEED_PRODUCTS:
        params = (
            p["sku"],
            p["name"],
            int(p.get("price") or 0),
            p.get("weight") or "",
            p.get("shelf_life") or "",
            p.get("description") or "",
            p.get("image_url") or "",
            p.get("badge") or "",
            _fixed_sort_for_sku(p["sku"], int(p.get("sort") or 0)),
            int(p.get("active") or 0),
        )
        if insert_only:
            cur.execute(
                """
                INSERT OR IGNORE INTO inventory
                (sku, name, stock, reserved, price, weight, shelf_life, description, image_url, badge, sort, active, catalog_override)
                VALUES (?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                params,
            )
        else:
            cur.execute(
                """
                INSERT INTO inventory
                (sku, name, stock, reserved, price, weight, shelf_life, description, image_url, badge, sort, active, catalog_override)
                VALUES (?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(sku) DO UPDATE SET
                  name=excluded.name,
                  price=excluded.price,
                  weight=excluded.weight,
                  shelf_life=excluded.shelf_life,
                  description=excluded.description,
                  image_url=excluded.image_url,
                  badge=excluded.badge,
                  sort=excluded.sort,
                  active=excluded.active,
                  catalog_override=1
                """,
                params,
            )
    con.commit()
    con.close()
RESERVE_MINUTES = 30

def expire_reservations() -> int:
    con = db()
    cur = con.cursor()

    rows = cur.execute("""
        SELECT order_id FROM reservations
        WHERE status='active' AND datetime(expires_at) <= datetime('now')
    """).fetchall()

    released = 0
    for r in rows:
        order_id = r["order_id"]
        items = cur.execute("""
            SELECT sku, qty FROM reservation_items WHERE order_id=?
        """, (order_id,)).fetchall()

        for it in items:
            cur.execute("""
                UPDATE inventory
                SET reserved = MAX(reserved - ?, 0)
                WHERE sku=?
            """, (it["qty"], it["sku"]))

        cur.execute("UPDATE reservations SET status='expired' WHERE order_id=?", (order_id,))
        released += 1

    con.commit()
    con.close()
    return released


def create_reservation(order_id: str, items: list[dict]) -> None:
    expire_reservations()

    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")

    for it in items:
        sku = str(it["sku"]).strip()
        qty = int(it["qty"])

        row = cur.execute("""
            SELECT stock, reserved FROM inventory WHERE sku=?
        """, (sku,)).fetchone()

        if not row:
            con.rollback()
            con.close()
            raise HTTPException(400, f"SKU not found: {sku}")

        available = int(row["stock"]) - int(row["reserved"])
        if qty > available:
            con.rollback()
            con.close()
            raise HTTPException(400, f"Not enough stock for {sku}. Available: {available}, requested: {qty}")

        cur.execute("UPDATE inventory SET reserved = reserved + ? WHERE sku=?", (qty, sku))

    cur.execute("""
        INSERT INTO reservations(order_id, status, expires_at)
        VALUES (?, 'active', datetime('now', ?))
    """, (order_id, f"+{RESERVE_MINUTES} minutes"))

    for it in items:
        cur.execute("""
            INSERT INTO reservation_items(order_id, sku, qty)
            VALUES (?, ?, ?)
        """, (order_id, it["sku"], int(it["qty"])))

    con.commit()
    con.close()


def mark_reservation_paid_and_deduct_stock(order_id: str) -> None:
    expire_reservations()

    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")

    res = cur.execute("SELECT status FROM reservations WHERE order_id=?", (order_id,)).fetchone()
    if not res:
        con.rollback()
        con.close()
        raise HTTPException(400, "Reservation not found")

    if res["status"] == "paid":
        con.commit()
        con.close()
        return

    if res["status"] in ("released", "expired"):
        con.rollback()
        con.close()
        raise HTTPException(409, f"Reservation already {res['status']}")

    items = cur.execute("SELECT sku, qty FROM reservation_items WHERE order_id=?", (order_id,)).fetchall()

    for it in items:
        sku = it["sku"]
        qty = int(it["qty"])
        row = cur.execute("SELECT stock, reserved FROM inventory WHERE sku=?", (sku,)).fetchone()
        stock = int(row["stock"])
        reserved = int(row["reserved"])

        cur.execute("""
            UPDATE inventory
            SET stock=?, reserved=?
            WHERE sku=?
        """, (max(stock - qty, 0), max(reserved - qty, 0), sku))

    cur.execute("UPDATE reservations SET status='paid' WHERE order_id=?", (order_id,))
    con.commit()
    con.close()


def release_reservation(order_id: str, reason: str = "released") -> None:
    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")

    res = cur.execute("SELECT status FROM reservations WHERE order_id=?", (order_id,)).fetchone()
    if not res or res["status"] != "active":
        con.rollback()
        con.close()
        return

    items = cur.execute("SELECT sku, qty FROM reservation_items WHERE order_id=?", (order_id,)).fetchall()
    for it in items:
        cur.execute("""
            UPDATE inventory SET reserved = MAX(reserved - ?, 0) WHERE sku=?
        """, (int(it["qty"]), it["sku"]))

    cur.execute("UPDATE reservations SET status=? WHERE order_id=?", (reason, order_id))
    con.commit()
    con.close()


def set_order_status(order_id: str, status: str) -> None:
    con = db()
    con.execute(
        "UPDATE orders SET status=?, updated_at=datetime('now') WHERE order_id=?",
        (status, order_id),
    )
    con.commit()
    con.close()


def get_order_payload(order_id: str) -> Optional[dict]:
    ensure_orders_columns()
    con = db()
    row = con.execute(
        """
        SELECT payload_json, amount, created_at, moysklad_demand_href, moysklad_sync_status,
               moysklad_sync_error, moysklad_synced_at
        FROM orders
        WHERE order_id=?
        """,
        (order_id,),
    ).fetchone()
    con.close()
    if not row:
        return None
    payload = json.loads(row["payload_json"])
    payload["_amount"] = row["amount"]
    payload["_created_at"] = row["created_at"]
    payload["_moysklad_demand_href"] = row["moysklad_demand_href"]
    payload["_moysklad_sync_status"] = row["moysklad_sync_status"]
    payload["_moysklad_sync_error"] = row["moysklad_sync_error"]
    payload["_moysklad_synced_at"] = row["moysklad_synced_at"]
    return payload


def claim_moysklad_sync(order_id: str) -> Optional[str]:
    ensure_orders_columns()
    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")
    row = cur.execute(
        """
        SELECT moysklad_demand_href, moysklad_sync_status
        FROM orders
        WHERE order_id=?
        """,
        (order_id,),
    ).fetchone()
    if not row:
        con.rollback()
        con.close()
        return "missing"
    demand_href = str(row["moysklad_demand_href"] or "").strip()
    sync_status = str(row["moysklad_sync_status"] or "").strip().lower()
    if demand_href:
        con.commit()
        con.close()
        return "done"
    if sync_status == "in_progress":
        con.commit()
        con.close()
        return "in_progress"
    cur.execute(
        """
        UPDATE orders
        SET moysklad_sync_status='in_progress',
            moysklad_sync_error='',
            updated_at=datetime('now')
        WHERE order_id=?
        """,
        (order_id,),
    )
    con.commit()
    con.close()
    return None


def finish_moysklad_sync(order_id: str, *, demand_href: str = "", error: str = "") -> None:
    ensure_orders_columns()
    con = db()
    con.execute(
        """
        UPDATE orders
        SET moysklad_demand_href=?,
            moysklad_sync_status=?,
            moysklad_sync_error=?,
            moysklad_synced_at=CASE WHEN ? <> '' THEN datetime('now') ELSE moysklad_synced_at END,
            updated_at=datetime('now')
        WHERE order_id=?
        """,
        (
            demand_href,
            "done" if demand_href else "error",
            error[:1000],
            demand_href,
            order_id,
        ),
    )
    con.commit()
    con.close()


def get_product_name_map(skus: list[str]) -> dict[str, str]:
    if not skus:
        return {}
    con = db()
    placeholders = ",".join("?" for _ in skus)
    rows = con.execute(
        f"SELECT sku, name FROM inventory WHERE sku IN ({placeholders})",
        skus,
    ).fetchall()
    con.close()
    return {r["sku"]: r["name"] for r in rows}


def _product_upload_public_url(filename: str) -> str:
    base = PUBLIC_BASE_URL.rstrip("/") if PUBLIC_BASE_URL else "http://127.0.0.1:8000"
    return f"{base}/uploads/products/{filename}"


def _safe_product_upload_name(filename: str) -> str:
    ext = os.path.splitext(filename or "")[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp"):
        raise HTTPException(400, "Поддерживаются только .jpg, .jpeg, .png, .webp")
    return f"{uuid.uuid4().hex}{ext}"


def upsert_product_card(payload: ProductCardUpsert) -> None:
    ensure_inventory_columns()
    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")

    existing = cur.execute("SELECT sku, stock, reserved FROM inventory WHERE sku=?", (payload.sku,)).fetchone()
    if existing:
        cur.execute(
            """
            UPDATE inventory
            SET name=?, price=?, weight=?, shelf_life=?, description=?, image_url=?,
                badge=?, sort=?, active=?, catalog_override=1
            WHERE sku=?
            """,
            (
                payload.name.strip(),
                int(payload.price),
                payload.weight.strip(),
                payload.shelfLife.strip(),
                payload.description.strip(),
                payload.imageUrl.strip(),
                payload.badge.strip(),
                int(payload.sort),
                int(payload.active),
                payload.sku.strip(),
            ),
        )
    else:
        cur.execute(
            """
            INSERT INTO inventory
            (sku, name, stock, reserved, price, weight, shelf_life, description, image_url, badge, sort, active, catalog_override)
            VALUES (?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                payload.sku.strip(),
                payload.name.strip(),
                int(payload.price),
                payload.weight.strip(),
                payload.shelfLife.strip(),
                payload.description.strip(),
                payload.imageUrl.strip(),
                payload.badge.strip(),
                int(payload.sort),
                int(payload.active),
            ),
        )

    con.commit()
    con.close()


def _leadteh_enabled() -> bool:
    return bool(LEADTEH_API_TOKEN and LEADTEH_BOT_ID)


def _leadteh_products_enabled() -> bool:
    return bool(LEADTEH_API_TOKEN and LEADTEH_PRODUCTS_SCHEMA_ID)


def _leadteh_request(client: httpx.Client, url: str, data: dict) -> dict:
    r = client.post(
        url,
        params={"api_token": LEADTEH_API_TOKEN},
        data=data,
        headers={"X-Requested-With": "XMLHttpRequest"},
        timeout=20,
    )
    try:
        return r.json()
    except Exception:
        return {"_status": r.status_code, "_text": r.text}


def _leadteh_get_list_items(schema_id: str) -> list[dict]:
    items: list[dict] = []
    if not schema_id:
        return items
    with httpx.Client() as client:
        page = 1
        while True:
            data = _leadteh_request(
                client,
                "https://app.leadteh.ru/api/v1/getListItems",
                {"schema_id": schema_id, "page": page},
            )
            chunk = data.get("data") or []
            if isinstance(chunk, dict):
                chunk = [chunk]
            items.extend(chunk)
            meta = data.get("meta") or {}
            last_page = meta.get("last_page") or meta.get("lastPage")
            if not last_page or page >= int(last_page):
                break
            page += 1
            time.sleep(0.6)
    return items


def _leadteh_bool(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    s = str(value).strip().lower()
    if s in ("1", "true", "yes", "y", "да"):
        return 1
    if s in ("0", "false", "no", "n", "нет"):
        return 0
    return 0


def _leadteh_int(value: Any) -> int:
    try:
        return int(float(str(value).replace(",", ".").strip()))
    except Exception:
        return 0


def _leadteh_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return value.get("url") or value.get("path") or value.get("value") or ""
    return str(value)


def sync_leadteh_products() -> dict:
    if not _leadteh_products_enabled():
        raise HTTPException(500, "Set LEADTEH_API_TOKEN and LEADTEH_PRODUCTS_SCHEMA_ID in backend/.env")

    items = _leadteh_get_list_items(LEADTEH_PRODUCTS_SCHEMA_ID)
    if not items:
        return {"ok": True, "updated": 0, "created": 0, "skipped": 0}

    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")

    updated = 0
    created = 0
    skipped = 0

    for item in items:
        sku = _leadteh_str(item.get("sku")).strip()
        if not sku:
            skipped += 1
            continue

        name = _leadteh_str(item.get("name"))
        price = _leadteh_int(item.get("price"))
        weight = _leadteh_str(item.get("weight"))
        shelf_life = _leadteh_str(item.get("shelf_life"))
        description = _leadteh_str(item.get("description"))
        image_url = _leadteh_str(item.get("image_url") or item.get("image"))
        badge = _leadteh_str(item.get("badge"))
        stock = _leadteh_int(item.get("stock"))
        sort = _leadteh_int(item.get("sort"))
        active = _leadteh_bool(item.get("active", 1))

        row = cur.execute(
            """
            SELECT sku, name, price, weight, shelf_life, description, image_url, badge, sort, active, catalog_override
            FROM inventory
            WHERE sku=?
            """,
            (sku,),
        ).fetchone()
        preserve_local_catalog = _catalog_override_enabled(row, sku)
        if row:
            name_value = _leadteh_str(row["name"]) if preserve_local_catalog else name
            price_value = _leadteh_int(row["price"]) if preserve_local_catalog else price
            weight_value = _leadteh_str(row["weight"]) if preserve_local_catalog else weight
            shelf_life_value = _leadteh_str(row["shelf_life"]) if preserve_local_catalog else shelf_life
            description_value = _leadteh_str(row["description"]) if preserve_local_catalog else description
            image_url_value = _leadteh_str(row["image_url"]) if preserve_local_catalog else image_url
            badge_value = _leadteh_str(row["badge"]) if preserve_local_catalog else badge
            sort_value = _leadteh_int(row["sort"]) if preserve_local_catalog else sort
            cur.execute(
                """
                UPDATE inventory
                SET name=?, price=?, weight=?, shelf_life=?, description=?, image_url=?,
                    badge=?, stock=?, sort=?, active=?
                WHERE sku=?
                """,
                (
                    name_value,
                    price_value,
                    weight_value,
                    shelf_life_value,
                    description_value,
                    image_url_value,
                    badge_value,
                    stock,
                    sort_value,
                    active,
                    sku,
                ),
            )
            updated += 1
        else:
            cur.execute(
                """
                INSERT INTO inventory
                (sku, name, stock, reserved, price, weight, shelf_life, description, image_url, badge, sort, active, catalog_override)
                VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (sku, name, stock, price, weight, shelf_life, description, image_url, badge, sort, active),
            )
            created += 1

    con.commit()
    con.close()
    return {"ok": True, "updated": updated, "created": created, "skipped": skipped}


def _inventory_rows_for_skus(skus: Optional[list[str]] = None) -> list[sqlite3.Row]:
    con = db()
    try:
        if skus:
            normalized = [str(sku).strip() for sku in skus if str(sku).strip()]
            if not normalized:
                return []
            placeholders = ",".join("?" for _ in normalized)
            rows = con.execute(
                f"""
                SELECT sku, name, price, weight, shelf_life, description, image_url, badge, sort, active, stock
                FROM inventory
                WHERE sku IN ({placeholders})
                """,
                normalized,
            ).fetchall()
            return rows

        rows = con.execute(
            """
            SELECT sku, name, price, weight, shelf_life, description, image_url, badge, sort, active, stock
            FROM inventory
            """
        ).fetchall()
        return rows
    finally:
        con.close()


def _push_inventory_rows_to_leadteh(rows: list[sqlite3.Row]) -> dict:
    if not _leadteh_products_enabled():
        raise HTTPException(500, "Set LEADTEH_API_TOKEN and LEADTEH_PRODUCTS_SCHEMA_ID in backend/.env")

    if not rows:
        return {"ok": True, "created": 0, "updated": 0}

    existing = _leadteh_get_list_items(LEADTEH_PRODUCTS_SCHEMA_ID)
    sku_to_id = {}
    for item in existing:
        sku = _leadteh_str(item.get("sku")).strip()
        item_id = item.get("id") or item.get("_id")
        if sku and item_id:
            sku_to_id[sku] = item_id

    created = 0
    updated = 0

    def to_form(data: dict) -> dict:
        out = {}
        for k, v in data.items():
            out[f"data[{k}]"] = "" if v is None else str(v)
        return out

    with httpx.Client() as client:
        for r in rows:
            payload = {
                "sku": r["sku"],
                "name": r["name"],
                "price": int(r["price"] or 0),
                "stock": int(r["stock"] or 0),
                "weight": r["weight"] or "",
                "shelf_life": r["shelf_life"] or "",
                "description": r["description"] or "",
                "image_url": r["image_url"] or "",
                "badge": r["badge"] or "",
                "sort": int(r["sort"] or 0),
                "active": int(r["active"] or 0),
            }
            sku = r["sku"]
            if sku in sku_to_id:
                data = {"item_id": sku_to_id[sku], **to_form(payload)}
                resp = _leadteh_request(client, "https://app.leadteh.ru/api/v1/updateListItem", data)
                if resp.get("data"):
                    updated += 1
            else:
                data = {"schema_id": LEADTEH_PRODUCTS_SCHEMA_ID, **to_form(payload)}
                resp = _leadteh_request(client, "https://app.leadteh.ru/api/v1/addListItem", data)
                if resp.get("data"):
                    created += 1
            time.sleep(0.6)

    return {"ok": True, "created": created, "updated": updated}


def push_products_to_leadteh() -> dict:
    return _push_inventory_rows_to_leadteh(_inventory_rows_for_skus())


def _sync_order_stocks_to_leadteh_sync(order_id: str) -> None:
    if not _leadteh_products_enabled():
        return

    payload = get_order_payload(order_id)
    if not payload:
        return

    skus = []
    seen = set()
    for item in payload.get("items") or []:
        sku = str(item.get("sku") or "").strip()
        if not sku or sku in seen:
            continue
        seen.add(sku)
        skus.append(sku)

    rows = _inventory_rows_for_skus(skus)
    result = _push_inventory_rows_to_leadteh(rows)
    print("Leadteh stock sync:", order_id, result)


async def sync_order_stocks_to_leadteh(order_id: str) -> None:
    try:
        await asyncio.to_thread(_sync_order_stocks_to_leadteh_sync, order_id)
    except Exception as e:
        print("Leadteh stock sync error:", repr(e))


def _sync_order_stocks_to_moysklad_sync(order_id: str) -> None:
    if not _moysklad_enabled():
        return

    claim_state = claim_moysklad_sync(order_id)
    if claim_state in ("done", "in_progress"):
        return
    if claim_state == "missing":
        raise ValueError(f"Order not found: {order_id}")

    payload = get_order_payload(order_id)
    if not payload:
        finish_moysklad_sync(order_id, error="Order payload not found")
        return

    sku_qty: dict[str, int] = {}
    for item in payload.get("items") or []:
        sku = str(item.get("sku") or "").strip()
        qty = int(item.get("qty") or 0)
        if not sku or qty <= 0:
            continue
        sku_qty[sku] = sku_qty.get(sku, 0) + qty

    if not sku_qty:
        finish_moysklad_sync(order_id, error="Order has no items for MoySklad sync")
        return

    con = db()
    placeholders = ",".join("?" for _ in sku_qty)
    rows = con.execute(
        f"""
        SELECT sku, name, price, moysklad_href
        FROM inventory
        WHERE sku IN ({placeholders})
        """,
        list(sku_qty.keys()),
    ).fetchall()
    con.close()

    row_map = {str(row["sku"]): row for row in rows}
    missing_skus = [sku for sku in sku_qty if sku not in row_map]
    unmapped_skus = [sku for sku, row in row_map.items() if not _moysklad_string(row["moysklad_href"])]
    if missing_skus:
        finish_moysklad_sync(order_id, error=f"Inventory rows not found for MoySklad sync: {', '.join(missing_skus)}")
        return
    if unmapped_skus:
        finish_moysklad_sync(order_id, error=f"Missing moysklad_href for SKU: {', '.join(unmapped_skus)}")
        return

    try:
        with httpx.Client() as client:
            organization_href, store_href = _moysklad_document_context(client)
            positions = []
            for sku, qty in sku_qty.items():
                row = row_map[sku]
                positions.append(
                    {
                        "quantity": qty,
                        "price": max(int(row["price"] or 0), 0) * 100,
                        "assortment": _moysklad_meta(_moysklad_string(row["moysklad_href"]), "product"),
                    }
                )

            customer = payload.get("customer") or {}
            delivery = payload.get("delivery") or {}
            demand_body = {
                "applicable": True,
                "moment": time.strftime("%Y-%m-%d %H:%M:%S"),
                "organization": _moysklad_meta(organization_href, "organization"),
                "store": _moysklad_meta(store_href, "store"),
                "description": (
                    f"Оплаченный заказ miniapp {order_id}. "
                    f"Клиент: {customer.get('name') or '-'}, "
                    f"телефон: {customer.get('phone') or '-'}, "
                    f"доставка: {delivery.get('method') or '-'}."
                ),
                "positions": positions,
            }
            result = _moysklad_request(client, "POST", "/entity/demand", json_body=demand_body)
    except Exception as exc:
        finish_moysklad_sync(order_id, error=repr(exc))
        raise

    demand_href = _moysklad_meta_href(result)
    if not demand_href:
        finish_moysklad_sync(order_id, error="MoySklad demand created without meta href")
        return

    finish_moysklad_sync(order_id, demand_href=demand_href)
    print("MoySklad stock sync:", order_id, demand_href)


async def sync_order_stocks_to_moysklad(order_id: str) -> None:
    try:
        await asyncio.to_thread(_sync_order_stocks_to_moysklad_sync, order_id)
    except Exception as e:
        print("MoySklad stock sync error:", repr(e))


def _moysklad_enabled() -> bool:
    return bool(MOYSKLAD_TOKEN)


def _moysklad_headers(accept: str = "application/json;charset=utf-8") -> dict:
    headers = {
        "Authorization": f"Bearer {MOYSKLAD_TOKEN}",
        "Accept": accept,
    }
    if accept.startswith("application/json"):
        headers["Content-Type"] = "application/json;charset=utf-8"
    return headers


def _moysklad_binary_headers() -> dict:
    return {"Authorization": f"Bearer {MOYSKLAD_TOKEN}"}


def _moysklad_host_allowed(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host == "moysklad.ru" or host.endswith(".moysklad.ru")


def _moysklad_request(
    client: httpx.Client,
    method: str,
    path_or_url: str,
    *,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
) -> dict:
    url = path_or_url if path_or_url.startswith("http") else f"{MOYSKLAD_API_BASE}{path_or_url}"
    r = client.request(
        method,
        url,
        params=params,
        json=json_body,
        headers=_moysklad_headers(),
        timeout=30,
    )
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as exc:
        body = (exc.response.text or "")[:400]
        raise HTTPException(exc.response.status_code, f"MoySklad API error: {body}")
    try:
        return r.json()
    except Exception:
        return {}


def _moysklad_get_rows(
    client: httpx.Client,
    path: str,
    *,
    params: Optional[dict] = None,
    limit: int = 100,
) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    base_params = dict(params or {})
    while True:
        req_params = {**base_params, "limit": limit, "offset": offset}
        data = _moysklad_request(client, "GET", path, params=req_params)
        chunk = data.get("rows") or []
        if not isinstance(chunk, list):
            chunk = []
        rows.extend(chunk)
        size = (data.get("meta") or {}).get("size")
        offset += len(chunk)
        if not chunk:
            break
        if size is not None:
            try:
                if offset >= int(size):
                    break
            except Exception:
                pass
        if len(chunk) < limit:
            break
    return rows


def _moysklad_meta(href: str, type_name: str) -> dict:
    return {
        "meta": {
            "href": href,
            "type": type_name,
            "mediaType": "application/json",
        }
    }


def _moysklad_meta_href(value: Any) -> str:
    if isinstance(value, dict):
        meta = value.get("meta") or {}
        href = _moysklad_string(meta.get("href"))
        if href:
            return href
        return _moysklad_string(value.get("href"))
    return _moysklad_string(value)


def _moysklad_first_entity_href(client: httpx.Client, path: str) -> str:
    rows = _moysklad_get_rows(client, path, limit=1)
    if not rows:
        return ""
    return _moysklad_meta_href(rows[0])


def _moysklad_document_context(client: httpx.Client) -> tuple[str, str]:
    organization_href = MOYSKLAD_ORGANIZATION_HREF
    store_href = MOYSKLAD_STORE_HREF

    try:
        metadata = _moysklad_request(client, "GET", "/entity/demand/metadata")
    except Exception:
        metadata = {}

    if not organization_href:
        organization_href = _moysklad_meta_href(metadata.get("organization"))
    if not store_href:
        store_href = _moysklad_meta_href(metadata.get("store"))
    if not organization_href:
        organization_href = _moysklad_first_entity_href(client, "/entity/organization")
    if not store_href:
        store_href = _moysklad_first_entity_href(client, "/entity/store")

    if not organization_href:
        raise ValueError("MoySklad organization not found. Set MOYSKLAD_ORGANIZATION_HREF in backend/.env")
    if not store_href:
        raise ValueError("MoySklad store not found. Set MOYSKLAD_STORE_HREF in backend/.env")

    return organization_href, store_href


def _moysklad_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return value.get("name") or value.get("value") or value.get("href") or ""
    return str(value).strip()


def _moysklad_int(value: Any) -> int:
    try:
        return int(round(float(str(value).replace(",", ".").strip())))
    except Exception:
        return 0


def _moysklad_int_or_none(value: Any) -> Optional[int]:
    s = _moysklad_string(value)
    if not s:
        return None
    try:
        return int(round(float(s.replace(",", "."))))
    except Exception:
        return None


def _moysklad_bool_or_none(value: Any) -> Optional[int]:
    s = _moysklad_string(value).lower()
    if not s:
        return None
    if s in ("1", "true", "yes", "y", "да"):
        return 1
    if s in ("0", "false", "no", "n", "нет"):
        return 0
    return None


def _moysklad_attr_rows(item: dict) -> list[dict]:
    attrs = item.get("attributes") or []
    if isinstance(attrs, dict):
        rows = attrs.get("rows") or []
        return rows if isinstance(rows, list) else []
    return attrs if isinstance(attrs, list) else []


def _moysklad_attr_value(item: dict, attr_name: str) -> str:
    target = (attr_name or "").strip().lower()
    if not target:
        return ""
    for attr in _moysklad_attr_rows(item):
        if _moysklad_string(attr.get("name")).lower() != target:
            continue
        value = attr.get("value")
        if isinstance(value, dict):
            return _moysklad_string(value)
        return _moysklad_string(value)
    return ""


def _moysklad_price(item: dict) -> int:
    prices = item.get("salePrices") or []
    if isinstance(prices, list) and prices:
        value = prices[0].get("value")
        if value is not None:
            return max(_moysklad_int(value) // 100, 0)
    return 0


def _moysklad_sku(item: dict) -> str:
    for candidate in ("article", "code", "externalCode", "id"):
        value = _moysklad_string(item.get(candidate))
        if value:
            return value
    return ""


def _moysklad_image_href(item: dict) -> str:
    image = item.get("image")
    if isinstance(image, dict):
        for key in ("downloadHref", "miniature", "href"):
            value = _moysklad_string(image.get(key))
            if value:
                return value
        meta = image.get("meta") or {}
        value = _moysklad_string(meta.get("href"))
        if value:
            return value

    images = item.get("images") or {}
    rows = images.get("rows") if isinstance(images, dict) else images
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in ("downloadHref", "miniature", "href"):
                value = _moysklad_string(row.get(key))
                if value:
                    return value
            meta = row.get("meta") or {}
            value = _moysklad_string(meta.get("href"))
            if value:
                return value
    return ""


def _moysklad_download_href(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("downloadHref", "miniature", "href"):
            candidate = value.get(key)
            if isinstance(candidate, dict):
                resolved = _moysklad_string(candidate)
            else:
                resolved = _moysklad_string(candidate)
            if resolved and _moysklad_host_allowed(resolved):
                return resolved

        meta = value.get("meta") or {}
        meta_href = _moysklad_string(meta.get("href"))
        meta_type = _moysklad_string(meta.get("mediaType")).lower()
        if meta_href and _moysklad_host_allowed(meta_href) and meta_type and not meta_type.startswith("application/json"):
            return meta_href

        image_href = _moysklad_image_href(value)
        if image_href and _moysklad_host_allowed(image_href):
            return image_href

        for key in ("rows", "images", "image"):
            nested = value.get(key)
            if isinstance(nested, list):
                for row in nested:
                    resolved = _moysklad_download_href(row)
                    if resolved:
                        return resolved
            else:
                resolved = _moysklad_download_href(nested)
                if resolved:
                    return resolved

    elif isinstance(value, list):
        for row in value:
            resolved = _moysklad_download_href(row)
            if resolved:
                return resolved

    return ""


def _absolute_public_url(path: str) -> str:
    if not path:
        return ""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if PUBLIC_BASE_URL:
        return f"{PUBLIC_BASE_URL.rstrip('/')}{path}"
    return path


def _moysklad_proxy_image_url(remote_href: str) -> str:
    if not remote_href or not _moysklad_host_allowed(remote_href):
        return ""
    return _absolute_public_url(f"/api/moysklad/image?{urlencode({'href': remote_href})}")


def _is_local_storefront_image_url(value: str) -> bool:
    url = _moysklad_string(value)
    if not url:
        return False
    if url.startswith("/products/"):
        return True
    parsed = urlparse(url)
    return parsed.path.startswith("/products/")


def sync_moysklad_products() -> dict:
    if not _moysklad_enabled():
        raise HTTPException(500, "Set MOYSKLAD_TOKEN in backend/.env")

    ensure_inventory_columns()

    con = None
    try:
        with httpx.Client() as client:
            items = _moysklad_get_rows(client, "/entity/product", params={"expand": "images"})

        if not items:
            return {"ok": True, "updated": 0, "created": 0, "skipped": 0}

        con = db()
        cur = con.cursor()
        cur.execute("BEGIN IMMEDIATE")

        updated = 0
        created = 0
        skipped = 0

        for item in items:
            sku = _moysklad_sku(item)
            name = _moysklad_string(item.get("name"))
            if not sku or not name:
                skipped += 1
                continue

            existing = cur.execute(
                """
                SELECT sku, name, price, stock, weight, shelf_life, description, image_url, badge, sort, active, catalog_override
                FROM inventory
                WHERE sku=?
                """,
                (sku,),
            ).fetchone()
            preserve_local_catalog = _catalog_override_enabled(existing, sku)

            attr_weight = _moysklad_attr_value(item, MOYSKLAD_ATTR_WEIGHT)
            attr_shelf_life = _moysklad_attr_value(item, MOYSKLAD_ATTR_SHELF_LIFE)
            attr_badge = _moysklad_attr_value(item, MOYSKLAD_ATTR_BADGE)
            attr_sort = _moysklad_attr_value(item, MOYSKLAD_ATTR_SORT)
            attr_active = _moysklad_attr_value(item, MOYSKLAD_ATTR_ACTIVE)
            attr_image_url = _moysklad_attr_value(item, MOYSKLAD_ATTR_IMAGE_URL)

            name_value = name
            if preserve_local_catalog:
                name_value = _moysklad_string(existing["name"]) or name

            weight_value = attr_weight or ""
            if not weight_value:
                standard_weight = _moysklad_int_or_none(item.get("weight"))
                if standard_weight:
                    weight_value = f"{standard_weight} г"
            if not weight_value and existing:
                weight_value = _moysklad_string(existing["weight"])
            if preserve_local_catalog:
                weight_value = _moysklad_string(existing["weight"]) or weight_value

            shelf_life_value = attr_shelf_life or (_moysklad_string(existing["shelf_life"]) if existing else "")
            badge_value = attr_badge or (_moysklad_string(existing["badge"]) if existing else "")
            if preserve_local_catalog:
                shelf_life_value = _moysklad_string(existing["shelf_life"]) or shelf_life_value
                badge_value = _moysklad_string(existing["badge"]) or badge_value

            sort_value = _moysklad_int_or_none(attr_sort)
            if sort_value is None:
                existing_sort = _moysklad_int(existing["sort"]) if existing else 0
                sort_value = _fixed_sort_for_sku(sku, existing_sort)

            active_value = _moysklad_bool_or_none(attr_active)
            if active_value is None:
                active_value = 0 if item.get("archived") else 1

            stock_value = _moysklad_int_or_none(item.get("stock"))
            if stock_value is None:
                stock_value = _moysklad_int_or_none(item.get("quantity"))
            if stock_value is None:
                stock_value = _moysklad_int(existing["stock"]) if existing else 0

            existing_image_url = _moysklad_string(existing["image_url"]) if existing else ""
            image_href = _moysklad_image_href(item)
            if preserve_local_catalog:
                image_url = existing_image_url or attr_image_url or _moysklad_proxy_image_url(image_href)
            elif existing_image_url and _is_local_storefront_image_url(existing_image_url) and not attr_image_url:
                image_url = existing_image_url
            else:
                image_url = attr_image_url or _moysklad_proxy_image_url(image_href) or existing_image_url

            description_value = _moysklad_string(item.get("description"))
            if not description_value and existing:
                description_value = _moysklad_string(existing["description"])
            if preserve_local_catalog:
                description_value = _moysklad_string(existing["description"]) or description_value

            moysklad_href = _moysklad_string(((item.get("meta") or {}).get("href")))
            price_value = _moysklad_price(item)
            if preserve_local_catalog:
                price_value = _moysklad_int(existing["price"]) or price_value

            if existing:
                cur.execute(
                    """
                    UPDATE inventory
                    SET name=?, price=?, weight=?, shelf_life=?, description=?, image_url=?,
                        badge=?, stock=?, sort=?, active=?, moysklad_href=?, moysklad_image_href=?
                    WHERE sku=?
                    """,
                    (
                        name_value,
                        price_value,
                        weight_value,
                        shelf_life_value,
                        description_value,
                        image_url,
                        badge_value,
                        stock_value,
                        sort_value,
                        active_value,
                        moysklad_href,
                        image_href,
                        sku,
                    ),
                )
                updated += 1
            else:
                cur.execute(
                    """
                    INSERT INTO inventory
                    (sku, name, stock, reserved, price, weight, shelf_life, description, image_url, badge, sort, active, catalog_override, moysklad_href, moysklad_image_href)
                    VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                    """,
                    (
                        sku,
                        name,
                        stock_value,
                        price_value,
                        weight_value,
                        shelf_life_value,
                        description_value,
                        image_url,
                        badge_value,
                        sort_value,
                        active_value,
                        moysklad_href,
                        image_href,
                    ),
                )
                created += 1

        con.commit()
        return {"ok": True, "updated": updated, "created": created, "skipped": skipped}
    except HTTPException:
        if con is not None:
            con.rollback()
        raise
    except Exception as e:
        if con is not None:
            con.rollback()
        print("MoySklad sync error:", repr(e))
        raise HTTPException(500, f"MoySklad sync error: {repr(e)}")
    finally:
        if con is not None:
            con.close()

def _normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D+", "", raw or "")
    if not digits:
        return ""
    # РФ: 11 цифр, 8XXXXXXXXXX -> 7XXXXXXXXXX
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 11 and digits.startswith("7"):
        return f"+{digits}"
    # если формат неизвестен — лучше не отправлять, чтобы не падало
    return ""


def _leadteh_phone_key(raw: str) -> str:
    digits = re.sub(r"\D+", "", raw or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return digits


def _leadteh_email_key(raw: str) -> str:
    return str(raw or "").strip().lower()


def _leadteh_get_contacts_page(client: httpx.Client, page: int, count: int = 500) -> dict:
    r = client.get(
        "https://app.leadteh.ru/api/v1/getContacts",
        params={
            "api_token": LEADTEH_API_TOKEN,
            "bot_id": LEADTEH_BOT_ID,
            "page": page,
            "count": count,
        },
        headers={"X-Requested-With": "XMLHttpRequest"},
        timeout=20,
    )
    try:
        return r.json()
    except Exception:
        return {"_status": r.status_code, "_text": r.text}


def _leadteh_find_contact_by_phone_or_email(client: httpx.Client, phone: str, email: str) -> Optional[int]:
    phone_key = _leadteh_phone_key(phone)
    email_key = _leadteh_email_key(email)
    if not phone_key and not email_key:
        return None

    page = 1
    email_match: Optional[int] = None

    while True:
        data = _leadteh_get_contacts_page(client, page=page)
        rows = data.get("data") or []
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            try:
                contact_id = int(row.get("id"))
            except Exception:
                continue

            row_phone = _leadteh_phone_key(str(row.get("phone") or ""))
            row_email = _leadteh_email_key(row.get("email") or "")

            if phone_key and row_phone and row_phone == phone_key:
                return contact_id
            if email_key and row_email and row_email == email_key and email_match is None:
                email_match = contact_id

        meta = data.get("meta") or {}
        try:
            current_page = int(meta.get("current_page") or page)
        except Exception:
            current_page = page
        try:
            last_page = int(meta.get("last_page") or current_page)
        except Exception:
            last_page = current_page

        if not rows or current_page >= last_page:
            break
        page = current_page + 1

    return email_match


def _leadteh_set_variable_sync(client: httpx.Client, contact_id: int, name: str, value: str) -> None:
    r = client.post(
        "https://app.leadteh.ru/api/v1/setContactVariable",
        params={
            "api_token": LEADTEH_API_TOKEN,
            "contact_id": contact_id,
            "name": name,
            "value": value,
        },
        data={"contact_id": contact_id, "name": name, "value": value},
        headers={"X-Requested-With": "XMLHttpRequest"},
        timeout=10,
    )
    print("Leadteh setVariable:", name, "status=", r.status_code, "body=", (r.text or "")[:200])


def _send_to_leadteh_sync(order_id: str) -> None:
    if not _leadteh_enabled():
        return

    payload = get_order_payload(order_id)
    if not payload:
        return

    customer = payload.get("customer") or {}
    delivery = payload.get("delivery") or {}
    items = payload.get("items") or []
    messenger_platform = str(payload.get("messenger_platform") or ("telegram" if payload.get("telegram_id") else "")).lower()
    messenger_user_id = str(payload.get("messenger_user_id") or "").strip()
    messenger_username = str(payload.get("messenger_username") or "").strip()
    telegram_id = payload.get("telegram_id")
    telegram_username = payload.get("telegram_username")

    skus = [str(it.get("sku")) for it in items if it.get("sku")]
    name_map = get_product_name_map(skus)
    items_text = "; ".join(
        f"{name_map.get(str(it.get('sku')), str(it.get('sku')))} × {it.get('qty', 1)}"
        for it in items
        if it.get("sku")
    )

    with httpx.Client() as client:
        phone = _normalize_phone(customer.get("phone", ""))
        contact_id = None
        contact_debug: Any = {}

        if messenger_platform == "telegram" and telegram_id:
            data_items = {
                "bot_id": LEADTEH_BOT_ID,
                "messenger": "telegram",
                "name": customer.get("name", "") or "Клиент",
                "email": customer.get("email", ""),
                "telegram_id": str(telegram_id),
                "telegram_username": telegram_username or "",
                "address": delivery.get("pickup_point", ""),
                "tags[]": "Оплата прошла",
            }
            if phone:
                data_items["phone"] = phone

            r = client.post(
                "https://app.leadteh.ru/api/v1/createOrUpdateContact",
                params={"api_token": LEADTEH_API_TOKEN},
                data=data_items,
                headers={"X-Requested-With": "XMLHttpRequest"},
                timeout=10,
            )
            print("Leadteh createOrUpdate:", r.status_code, (r.text or "")[:200])
            try:
                data = r.json()
            except Exception:
                data = {}
            contact_debug = data
            contact_id = data.get("data", {}).get("id")
        elif messenger_platform == "max":
            contact_id = _leadteh_find_contact_by_phone_or_email(
                client,
                phone,
                customer.get("email", ""),
            )
            contact_debug = {
                "phone": phone,
                "email": customer.get("email", ""),
                "contact_id": contact_id,
            }
            print(
                "Leadteh MAX contact lookup:",
                contact_debug,
            )
        else:
            return

        if not contact_id:
            print("Leadteh: no contact_id in response", contact_debug)
            return

        variables = [
            ("customer_name", customer.get("name", "")),
            ("customer_email", customer.get("email", "")),
            ("customer_phone", phone or customer.get("phone", "")),
            ("customer_address", delivery.get("pickup_point", "")),
            ("messenger_platform", messenger_platform),
            ("messenger_user_id", messenger_user_id),
            ("messenger_username", messenger_username or (telegram_username or "")),
            ("order_id", order_id),
            ("amount", str(payload.get("_amount", ""))),
            ("items", items_text),
            ("delivery_method", delivery.get("method", "")),
            ("pickup_point", delivery.get("pickup_point", "")),
            ("comment", payload.get("comment", "")),
            ("payment_status", "success"),
            ("payment_note", "Оплачено"),
            ("order_created_at", str(payload.get("_created_at", ""))),
        ]

        for name, value in variables:
            _leadteh_set_variable_sync(client, contact_id, name, value or "")
            time.sleep(0.6)


async def send_to_leadteh(order_id: str) -> None:
    try:
        await asyncio.to_thread(_send_to_leadteh_sync, order_id)
    except Exception as e:
        print("Leadteh error:", repr(e))


# ---------------------------
# Prodamus signature helpers
# ---------------------------
def _to_str_deep(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _to_str_deep(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_str_deep(v) for v in obj]
    if obj is None:
        return ""
    return str(obj)


def _sort_keys_deep(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _sort_keys_deep(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, list):
        return [_sort_keys_deep(v) for v in obj]
    return obj


def normalize_newlines_deep(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: normalize_newlines_deep(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_newlines_deep(v) for v in obj]
    if isinstance(obj, str):
        return obj.replace("\r\n", "\n").replace("\r", "\n")
    return obj


def prodamus_sign_unicode(data: Dict[str, Any], secret_key: str) -> str:
    """
    Вариант JSON как у нас раньше (кириллица не экранируется).
    """
    prepared = _sort_keys_deep(_to_str_deep(data))
    s = json.dumps(prepared, ensure_ascii=False, separators=(",", ":"))
    s = s.replace("/", r"\/")
    return hmac.new(secret_key.encode("utf-8"), s.encode("utf-8"), hashlib.sha256).hexdigest()


def prodamus_sign_ascii(data: Dict[str, Any], secret_key: str) -> str:
    """
Вариант JSON как в стандартном json_encode PHP (кириллица экранируется \\uXXXX).
"""

    prepared = _sort_keys_deep(_to_str_deep(data))
    s = json.dumps(prepared, ensure_ascii=True, separators=(",", ":"))
    s = s.replace("/", r"\/")
    return hmac.new(secret_key.encode("utf-8"), s.encode("utf-8"), hashlib.sha256).hexdigest()

def prodamus_sign(data: Dict[str, Any], secret_key: str) -> str:
    mode = PRODAMUS_SIGN_MODE
    if mode == "unicode":
        return prodamus_sign_unicode(data, secret_key)
    return prodamus_sign_ascii(data, secret_key)


def flatten_for_prodamus(data: dict) -> dict:
    """
    Превращает вложенные структуры в формат products[0][name]... для Payform.
    """
    out: Dict[str, str] = {}
    for k, v in data.items():
        if isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    for kk, vv in item.items():
                        out[f"{k}[{i}][{kk}]"] = "" if vv is None else str(vv)
                else:
                    out[f"{k}[{i}]"] = "" if item is None else str(item)
        elif isinstance(v, dict):
            for kk, vv in v.items():
                out[f"{k}[{kk}]"] = "" if vv is None else str(vv)
        else:
            out[k] = "" if v is None else str(v)
    return out


def build_prodamus_url(base_url: str, form_data: dict) -> str:
    if not base_url:
        return ""
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}{urlencode(form_data)}"


def _prodamus_signature_variants(data_for_pay: Dict[str, Any], secret_key: str) -> List[Tuple[str, str]]:
    flat = flatten_for_prodamus(data_for_pay)
    return [
        ("nested_ascii", prodamus_sign_ascii(data_for_pay, secret_key)),
        ("nested_unicode", prodamus_sign_unicode(data_for_pay, secret_key)),
        ("flat_ascii", prodamus_sign_ascii(flat, secret_key)),
        ("flat_unicode", prodamus_sign_unicode(flat, secret_key)),
    ]


def _prodamus_pay_url(base_url: str, data_for_pay: Dict[str, Any], signature: str) -> str:
    payload = dict(data_for_pay)
    payload["signature"] = signature
    return build_prodamus_url(base_url, flatten_for_prodamus(payload))


async def _prodamus_link_seems_valid(url: str) -> bool:
    if not url:
        return False
    try:
        async with httpx.AsyncClient(timeout=PRODAMUS_AUTO_SIGN_TIMEOUT, follow_redirects=False) as client:
            r = await client.get(url)
    except Exception as exc:
        print("Prodamus validate error:", repr(exc))
        return False

    # Обычно при ошибке подписи идёт редирект на корень формы.
    if r.status_code in (301, 302, 303, 307, 308):
        loc = (r.headers.get("location") or r.headers.get("Location") or "").strip()
        if loc:
            base = PRODAMUS_FORM_URL.rstrip("/")
            if loc.rstrip("/") == base:
                return False
        return True

    body = (r.text or "")
    if "Ошибка подписи" in body:
        return False
    return r.status_code < 400


# ---------------------------
# Unflatten products[0][name] -> products: [{name:...}]
# ---------------------------
_bracket_re = re.compile(r"^([^\[]+)((?:\[[^\]]*\])+)")

def unflatten_brackets(flat: dict) -> dict:
    root: dict = {}

    def ensure_list_size(lst: list, idx: int):
        while len(lst) <= idx:
            lst.append({})

    for key, value in flat.items():
        m = _bracket_re.match(key)
        if not m:
            root[key] = value
            continue

        base = m.group(1)
        parts = re.findall(r"\[([^\]]*)\]", m.group(2))

        cur = root
        if base not in cur:
            cur[base] = [] if (parts and parts[0].isdigit()) else {}
        cur = cur[base]

        for i, p in enumerate(parts):
            is_last = (i == len(parts) - 1)

            if isinstance(cur, list):
                if not p.isdigit():
                    raise ValueError(f"Expected list index in key {key}, got {p}")
                idx = int(p)
                ensure_list_size(cur, idx)

                if is_last:
                    cur[idx] = value
                else:
                    if not isinstance(cur[idx], (dict, list)):
                        cur[idx] = {}
                    cur = cur[idx]
            else:
                if is_last:
                    cur[p] = value
                else:
                    nxt_is_index = parts[i + 1].isdigit()
                    if p not in cur or not isinstance(cur[p], (dict, list)):
                        cur[p] = [] if nxt_is_index else {}
                    cur = cur[p]

    return root


# ---------------------------
# FastAPI app
# ---------------------------
app = FastAPI(title="MiniApp Shop Backend")
init_db()
security = HTTPBasic()


def require_admin(credentials: HTTPBasicCredentials = Depends(security)) -> None:
    if not ADMIN_USER or not ADMIN_PASS:
        raise HTTPException(500, "Set ADMIN_USER and ADMIN_PASS in backend/.env")
    ok_user = secrets.compare_digest(credentials.username or "", ADMIN_USER)
    ok_pass = secrets.compare_digest(credentials.password or "", ADMIN_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://miniapp-shop-one.vercel.app",
        "https://egorrnd.ru",
        "https://www.egorrnd.ru",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup():
    init_db()


@app.get("/")
def root():
    return {"ok": True, "hint": "Use /docs or /health"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/moysklad/image")
def moysklad_image_proxy(href: str):
    if not _moysklad_enabled():
        raise HTTPException(500, "Set MOYSKLAD_TOKEN in backend/.env")
    if not href or not _moysklad_host_allowed(href):
        raise HTTPException(400, "Invalid image href")

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        current_href = href
        r = client.get(current_href, headers=_moysklad_binary_headers())

        # Some MoySklad image links first return JSON metadata, not the file itself.
        if (r.headers.get("content-type") or "").lower().startswith("application/json"):
            try:
                payload = r.json()
            except Exception:
                payload = {}
            resolved_href = _moysklad_download_href(payload)
            if resolved_href and resolved_href != current_href:
                current_href = resolved_href
                r = client.get(current_href, headers=_moysklad_binary_headers())

    if r.status_code >= 400:
        raise HTTPException(r.status_code, "MoySklad image fetch failed")

    media_type = r.headers.get("content-type") or "application/octet-stream"
    return Response(
        content=r.content,
        media_type=media_type,
        headers={"Cache-Control": "public, max-age=3600"},
    )


# ---------------------------
# Models
# ---------------------------
class Item(BaseModel):
    sku: str
    qty: int = Field(ge=1, le=50)


class Customer(BaseModel):
    name: str
    email: str
    phone: str


class Delivery(BaseModel):
    method: str  # cdek|ozon|wildberries
    pickup_point: str


class OrderIn(BaseModel):
    initData: str = ""
    messenger_platform: Optional[str] = None
    messenger_user_id: Optional[str] = None
    messenger_username: Optional[str] = None
    telegram_id: Optional[int] = None
    telegram_username: Optional[str] = None
    customer: Customer
    delivery: Delivery
    comment: Optional[str] = ""
    items: List[Item]


class InventoryUpdate(BaseModel):
    sku: str
    stock: int = Field(ge=0)


class InventoryUpdateBulk(BaseModel):
    items: List[InventoryUpdate]


class ProductCardUpsert(BaseModel):
    sku: str
    name: str
    price: int = Field(ge=0)
    weight: str = ""
    shelfLife: str = ""
    description: str = ""
    imageUrl: str = ""
    badge: str = ""
    sort: int = 0
    active: int = Field(default=1, ge=0, le=1)


# ---------------------------
# Create order -> Prodamus payment link
# ---------------------------
@app.post("/api/orders")
async def create_order(order: OrderIn):
    if not (PRODAMUS_FORM_URL and PRODAMUS_SYS and PRODAMUS_SECRET_KEY):
        raise HTTPException(
            500,
            "Set PRODAMUS_FORM_URL, PRODAMUS_SYS, PRODAMUS_SECRET_KEY in backend/.env",
        )

    order_uuid = str(uuid.uuid4())  # это ваш order_num в Prodamus webhook
    create_reservation(
        order_uuid,
        [{"sku": it.sku, "qty": it.qty} for it in order.items]
    )
    products: List[Dict[str, Any]] = []
    amount = 0

    con = db()
    for it in order.items:
        row = con.execute(
            "SELECT name, price, active FROM inventory WHERE sku=?",
            (it.sku,),
        ).fetchone()
        if not row or int(row["active"] or 0) == 0:
            con.close()
            raise HTTPException(400, f"Unknown or inactive sku: {it.sku}")
        price = int(row["price"] or 0)
        if price <= 0:
            con.close()
            raise HTTPException(400, f"Price not set for sku: {it.sku}")
        products.append(
            {
                "name": row["name"],
                "price": price,
                "quantity": it.qty,
            }
        )
        amount += price * it.qty
    con.close()

    customer_extra = (
        f"Имя: {order.customer.name}\n"
        f"Email: {order.customer.email}\n"
        f"Телефон: {order.customer.phone}\n"
        f"Доставка: {order.delivery.method}\n"
        f"Пункт выдачи: {order.delivery.pickup_point}\n"
        f"Комментарий: {order.comment or ''}"
    ).strip()

    # Телефон: по умолчанию приводим к цифрам (частое требование Prodamus).
    raw_phone = (order.customer.phone or "").strip()
    if PRODAMUS_PHONE_DIGITS:
        customer_phone = re.sub(r"\D+", "", raw_phone)
        if customer_phone.startswith("8") and len(customer_phone) == 11:
            customer_phone = "7" + customer_phone[1:]
    else:
        customer_phone = raw_phone

    # Базовый payload для Prodamus.
    if PRODAMUS_AMOUNT_ONLY:
        # Оставляем только сумму в виде одного товара (Prodamus требует products).
        products_payload = [
            {
                "name": PRODAMUS_SINGLE_PRODUCT_NAME or "Оплата заказа",
                "price": amount,
                "quantity": 1,
            }
        ]
    else:
        products_payload = products

    # Для подписи products держим как list (как мы формируем)
    base_payload: Dict[str, Any] = {
        "sys": PRODAMUS_SYS,
        "products": products_payload,
    }
    if not PRODAMUS_NO_ORDER_ID:
        # Передаём сразу оба поля — Prodamus обычно возвращает order_num (наша система)
        # и order_id (их внутренний id). Это повышает шанс корректного сопоставления.
        base_payload["order_id"] = order_uuid  # номер заказа в нашей системе
        base_payload["order_num"] = order_uuid
    if not PRODAMUS_MINIMAL and not PRODAMUS_AMOUNT_ONLY:
        base_payload["customer_phone"] = customer_phone
        base_payload["customer_email"] = order.customer.email
        if PRODAMUS_INCLUDE_EXTRA:
            base_payload["customer_extra"] = customer_extra

    payment_url = ""
    if not PRODAMUS_DIRECT_ONLY:
        data_for_sign: Dict[str, Any] = {**base_payload, "do": "link"}
        # подпись (в запросе на создание ссылки)
        sign_payload = data_for_sign
        if PRODAMUS_SIGN_SOURCE == "flat":
            sign_payload = flatten_for_prodamus(data_for_sign)
        data_for_sign["signature"] = prodamus_sign(sign_payload, PRODAMUS_SECRET_KEY)

        # Payform ждёт плоский формат products[0][...]
        form_data = flatten_for_prodamus(data_for_sign)

        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
                r = await client.post(PRODAMUS_FORM_URL, data=form_data)

            body = (r.text or "").strip()
            if r.status_code in (301, 302, 303, 307, 308):
                loc = r.headers.get("location") or r.headers.get("Location")
                if loc:
                    payment_url = loc.strip()
            if not payment_url and body.startswith("http"):
                payment_url = body
            if not payment_url:
                m = re.search(r"url=([^\"'>\\s]+)", body, flags=re.IGNORECASE)
                if m:
                    payment_url = m.group(1).strip()
        except Exception as exc:
            print("Prodamus link request failed, falling back to direct link:", repr(exc))

    # Прямая ссылка оплаты с параметрами — полезно, если на странице не подхватываются данные.
    data_for_pay = {**base_payload, "do": "pay"}
    payment_url_direct = ""
    if PRODAMUS_AUTO_SIGN:
        for variant, signature in _prodamus_signature_variants(data_for_pay, PRODAMUS_SECRET_KEY):
            url_try = _prodamus_pay_url(PRODAMUS_FORM_URL, data_for_pay, signature)
            if await _prodamus_link_seems_valid(url_try):
                payment_url_direct = url_try
                print("Prodamus auto-sign ok:", variant)
                break
        if not payment_url_direct:
            print("Prodamus auto-sign failed, falling back to configured mode")

    if not payment_url_direct:
        pay_sign_payload = data_for_pay
        if PRODAMUS_SIGN_SOURCE == "flat":
            pay_sign_payload = flatten_for_prodamus(data_for_pay)
        signature = prodamus_sign(pay_sign_payload, PRODAMUS_SECRET_KEY)
        payment_url_direct = _prodamus_pay_url(PRODAMUS_FORM_URL, data_for_pay, signature)

    def is_empty_payform_link(url: str) -> bool:
        if not url:
            return True
        try:
            parsed = urlparse(url)
        except Exception:
            return True
        return (parsed.path in ("", "/")) and not parsed.query

    if not payment_url or is_empty_payform_link(payment_url):
        payment_url = payment_url_direct

    con = db()
    con.execute(
        "INSERT INTO orders(order_id,status,amount,payload_json,payment_url) VALUES (?,?,?,?,?)",
        (order_uuid, "created", amount, json.dumps(order.model_dump(), ensure_ascii=False), payment_url_direct or payment_url),
    )
    con.commit()
    con.close()

    return {
        "order_id": order_uuid,
        "payment_url": payment_url,
        "payment_url_direct": payment_url_direct,
        "amount": amount
    }

@app.get("/api/orders/{order_id}")
def get_order(order_id: str):
    con = db()
    row = con.execute(
        "SELECT order_id, status, amount, payment_url, created_at, updated_at FROM orders WHERE order_id=?",
        (order_id,),
    ).fetchone()
    con.close()

    if not row:
        raise HTTPException(404, "Order not found")

    return dict(row)

# ---------------------------
# Prodamus webhook
# ---------------------------
@app.post("/api/prodamus/webhook")
async def prodamus_webhook(request: Request):
    """
    Сюда приходит webhook от Prodamus.
    Подпись в заголовке Sign.
    """

    sign = request.headers.get("Sign") or request.headers.get("sign")
    if not sign:
        raise HTTPException(400, "Missing Sign header")

    content_type = (request.headers.get("content-type") or "").lower()

    if "application/json" in content_type:
        payload = await request.json()
        flat_payload = payload
    else:
        form = await request.form()
        flat_payload = dict(form)

    nested_payload = unflatten_brackets(flat_payload)

    print("=== PRODAMUS WEBHOOK RECEIVED ===")
    print("Sign header:", sign)
    print("Payload flat:", flat_payload)
    print("Payload nested:", nested_payload)

    # ---- считаем 8 вариантов подписи ----
    # unicode JSON
    calc_flat_raw_u = prodamus_sign_unicode(flat_payload, PRODAMUS_SECRET_KEY)
    calc_flat_norm_u = prodamus_sign_unicode(normalize_newlines_deep(flat_payload), PRODAMUS_SECRET_KEY)
    calc_nested_raw_u = prodamus_sign_unicode(nested_payload, PRODAMUS_SECRET_KEY)
    calc_nested_norm_u = prodamus_sign_unicode(normalize_newlines_deep(nested_payload), PRODAMUS_SECRET_KEY)

    # ascii JSON
    calc_flat_raw_a = prodamus_sign_ascii(flat_payload, PRODAMUS_SECRET_KEY)
    calc_flat_norm_a = prodamus_sign_ascii(normalize_newlines_deep(flat_payload), PRODAMUS_SECRET_KEY)
    calc_nested_raw_a = prodamus_sign_ascii(nested_payload, PRODAMUS_SECRET_KEY)
    calc_nested_norm_a = prodamus_sign_ascii(normalize_newlines_deep(nested_payload), PRODAMUS_SECRET_KEY)

    print("Calc flat raw u   :", calc_flat_raw_u)
    print("Calc flat norm u  :", calc_flat_norm_u)
    print("Calc nested raw u :", calc_nested_raw_u)
    print("Calc nested norm u:", calc_nested_norm_u)
    print("Calc flat raw a   :", calc_flat_raw_a)
    print("Calc flat norm a  :", calc_flat_norm_a)
    print("Calc nested raw a :", calc_nested_raw_a)
    print("Calc nested norm a:", calc_nested_norm_a)

    sign = sign.strip()
    ok = any(
        hmac.compare_digest(x, sign)
        for x in [
            calc_flat_raw_u,
            calc_flat_norm_u,
            calc_nested_raw_u,
            calc_nested_norm_u,
            calc_flat_raw_a,
            calc_flat_norm_a,
            calc_nested_raw_a,
            calc_nested_norm_a,
        ]
    )

    if not ok:
        # ВАЖНО: если 401 — Prodamus будет ретраить
        raise HTTPException(401, "Invalid signature")

    # В webhook у вас реально приходит:
    # order_id (id Prodamus) и order_num (ваш UUID).
    # Нам нужно обновлять БД по вашему UUID.
    order_uuid = str(flat_payload.get("order_num") or nested_payload.get("order_num") or "")
    if not order_uuid:
        order_uuid = str(flat_payload.get("order_id") or nested_payload.get("order_id") or "")

    payment_status = str(flat_payload.get("payment_status") or nested_payload.get("payment_status") or "").lower()

    print("order_uuid:", order_uuid)
    print("payment_status:", payment_status)

    try:
        if payment_status == "success":
            mark_reservation_paid_and_deduct_stock(order_uuid)
            set_order_status(order_uuid, "paid")
            asyncio.create_task(send_to_leadteh(order_uuid))
            asyncio.create_task(sync_order_stocks_to_leadteh(order_uuid))
            asyncio.create_task(sync_order_stocks_to_moysklad(order_uuid))
        else:
            release_reservation(order_uuid, reason=payment_status or "released")
            set_order_status(order_uuid, payment_status or "unknown")
    except Exception as e:
        print("DB ERROR:", repr(e))
        raise HTTPException(500, f"DB error: {repr(e)}")

    return {"ok": True}
@app.get("/api/inventory")
def get_inventory():
    expire_reservations()
    con = db()
    rows = con.execute("""
        SELECT sku, name, stock, reserved, (stock - reserved) AS available
        FROM inventory
        ORDER BY name
    """).fetchall()
    con.close()
    return [dict(r) for r in rows]


@app.patch("/api/inventory")
def update_inventory(payload: InventoryUpdateBulk, _: None = Depends(require_admin)):
    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")

    for it in payload.items:
        cur.execute(
            "UPDATE inventory SET stock=? WHERE sku=?",
            (int(it.stock), str(it.sku)),
        )

    con.commit()
    con.close()
    return {"ok": True}


@app.get("/api/products")
def get_products():
    expire_reservations()
    con = db()
    rows = con.execute(
        """
        SELECT
          sku,
          name,
          price,
          weight,
          shelf_life AS shelfLife,
          description,
          image_url AS imageUrl,
          badge,
          sort,
          active,
          catalog_override AS catalogOverride,
          stock,
          reserved,
          (stock - reserved) AS available
        FROM inventory
        ORDER BY sort ASC, name ASC
        """
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]


@app.get("/uploads/products/{filename}")
def get_uploaded_product_image(filename: str):
    safe_name = os.path.basename(filename)
    path = os.path.join(PRODUCT_UPLOADS_DIR, safe_name)
    if not os.path.isfile(path):
        raise HTTPException(404, "Image not found")
    return FileResponse(path)


@app.put("/api/products/{sku}")
def upsert_product_card_endpoint(sku: str, payload: ProductCardUpsert, _: None = Depends(require_admin)):
    normalized_sku = sku.strip()
    if normalized_sku != payload.sku.strip():
        raise HTTPException(400, "SKU in path and body must match")
    upsert_product_card(payload)
    return {"ok": True}


@app.post("/api/products/image")
async def upload_product_image(file: UploadFile = File(...), _: None = Depends(require_admin)):
    os.makedirs(PRODUCT_UPLOADS_DIR, exist_ok=True)
    if not (file.content_type or "").startswith("image/"):
        raise HTTPException(400, "Можно загружать только изображения")

    safe_name = _safe_product_upload_name(file.filename or "image.jpg")
    path = os.path.join(PRODUCT_UPLOADS_DIR, safe_name)

    with open(path, "wb") as out:
        shutil.copyfileobj(file.file, out)

    return {"ok": True, "imageUrl": _product_upload_public_url(safe_name), "filename": safe_name}


@app.post("/api/leadteh/sync")
def sync_products(_: None = Depends(require_admin)):
    return sync_leadteh_products()


@app.post("/api/moysklad/sync")
def sync_products_moysklad(_: None = Depends(require_admin)):
    return sync_moysklad_products()


@app.post("/api/leadteh/push")
def push_products(_: None = Depends(require_admin)):
    return push_products_to_leadteh()


@app.post("/api/products/seed")
def seed_products_endpoint(_: None = Depends(require_admin)):
    seed_products(insert_only=False)
    return {"ok": True}
