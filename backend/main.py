import os
import json
import hmac
import hashlib
import sqlite3
import uuid
import re
import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

import httpx
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Request, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
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

if PRODAMUS_FORM_URL and not PRODAMUS_FORM_URL.endswith("/"):
    PRODAMUS_FORM_URL += "/"

DB_PATH = os.path.join(os.path.dirname(__file__), "app.db")

# ---- сиды каталога (пока Leadteh не настроен) ----
SEED_PRODUCTS = [
    {
        "sku": "GIFT_BOX_LOVED",
        "name": "Подарочный бокс «Это любят Люди»",
        "price": 4700,
        "weight": "",
        "shelf_life": "",
        "badge": "ХИТ продаж!",
        "image_url": "/products/box.jpg",
        "description": (
            "1. Дубайский шоколад — 1 шт.\n"
            "2. Пирожное «Картошка» — 1 шт.\n"
            "3. Набор конфет ассорти «В самое сердце» — 1 шт.\n"
            "4. Конфеты «Орешек» — 1 шт.\n"
            "Бонус: плитка шоколада — 1 шт."
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
]


# ---------------------------
# DB
# ---------------------------
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
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
    ensure_inventory_columns()
    seed_products(insert_only=True)
    return

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
            int(p.get("sort") or 0),
            int(p.get("active") or 0),
        )
        if insert_only:
            cur.execute(
                """
                INSERT OR IGNORE INTO inventory
                (sku, name, stock, reserved, price, weight, shelf_life, description, image_url, badge, sort, active)
                VALUES (?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )
        else:
            cur.execute(
                """
                INSERT INTO inventory
                (sku, name, stock, reserved, price, weight, shelf_life, description, image_url, badge, sort, active)
                VALUES (?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sku) DO UPDATE SET
                  name=excluded.name,
                  price=excluded.price,
                  weight=excluded.weight,
                  shelf_life=excluded.shelf_life,
                  description=excluded.description,
                  image_url=excluded.image_url,
                  badge=excluded.badge,
                  sort=excluded.sort,
                  active=excluded.active
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
    con = db()
    row = con.execute(
        "SELECT payload_json, amount, created_at FROM orders WHERE order_id=?",
        (order_id,),
    ).fetchone()
    con.close()
    if not row:
        return None
    payload = json.loads(row["payload_json"])
    payload["_amount"] = row["amount"]
    payload["_created_at"] = row["created_at"]
    return payload


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

        row = cur.execute("SELECT sku FROM inventory WHERE sku=?", (sku,)).fetchone()
        if row:
            cur.execute(
                """
                UPDATE inventory
                SET name=?, price=?, weight=?, shelf_life=?, description=?, image_url=?,
                    badge=?, stock=?, sort=?, active=?
                WHERE sku=?
                """,
                (name, price, weight, shelf_life, description, image_url, badge, stock, sort, active, sku),
            )
            updated += 1
        else:
            cur.execute(
                """
                INSERT INTO inventory
                (sku, name, stock, reserved, price, weight, shelf_life, description, image_url, badge, sort, active)
                VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (sku, name, stock, price, weight, shelf_life, description, image_url, badge, sort, active),
            )
            created += 1

    con.commit()
    con.close()
    return {"ok": True, "updated": updated, "created": created, "skipped": skipped}


def push_products_to_leadteh() -> dict:
    if not _leadteh_products_enabled():
        raise HTTPException(500, "Set LEADTEH_API_TOKEN and LEADTEH_PRODUCTS_SCHEMA_ID in backend/.env")

    existing = _leadteh_get_list_items(LEADTEH_PRODUCTS_SCHEMA_ID)
    sku_to_id = {}
    for item in existing:
        sku = _leadteh_str(item.get("sku")).strip()
        item_id = item.get("id") or item.get("_id")
        if sku and item_id:
            sku_to_id[sku] = item_id

    con = db()
    rows = con.execute(
        """
        SELECT sku, name, price, weight, shelf_life, description, image_url, badge, sort, active, stock
        FROM inventory
        """
    ).fetchall()
    con.close()

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
    telegram_id = payload.get("telegram_id")
    telegram_username = payload.get("telegram_username")

    if not telegram_id:
        return

    skus = [str(it.get("sku")) for it in items if it.get("sku")]
    name_map = get_product_name_map(skus)
    items_text = "; ".join(
        f"{name_map.get(str(it.get('sku')), str(it.get('sku')))} × {it.get('qty', 1)}"
        for it in items
        if it.get("sku")
    )

    with httpx.Client() as client:
        phone = _normalize_phone(customer.get("phone", ""))
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
        contact_id = data.get("data", {}).get("id")
        if not contact_id:
            print("Leadteh: no contact_id in response", data)
            return

        variables = [
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
        else:
            release_reservation(order_uuid, reason=payment_status or "released")
            set_order_status(order_uuid, payment_status or "unknown")

            set_order_status(order_uuid, "paid")
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
          stock,
          reserved,
          (stock - reserved) AS available
        FROM inventory
        ORDER BY sort ASC, name ASC
        """
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]


@app.post("/api/leadteh/sync")
def sync_products(_: None = Depends(require_admin)):
    return sync_leadteh_products()


@app.post("/api/leadteh/push")
def push_products(_: None = Depends(require_admin)):
    return push_products_to_leadteh()


@app.post("/api/products/seed")
def seed_products_endpoint(_: None = Depends(require_admin)):
    seed_products(insert_only=False)
    return {"ok": True}
