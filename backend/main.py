import os
import json
import hmac
import hashlib
import sqlite3
import uuid
import re
import asyncio
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Request, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
import secrets

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

PRODAMUS_FORM_URL = os.getenv("PRODAMUS_FORM_URL", "").strip()
PRODAMUS_SYS = os.getenv("PRODAMUS_SYS", "").strip()
PRODAMUS_SECRET_KEY = os.getenv("PRODAMUS_SECRET_KEY", "").strip()
ADMIN_USER = os.getenv("ADMIN_USER", "").strip()
ADMIN_PASS = os.getenv("ADMIN_PASS", "").strip()
LEADTEH_API_TOKEN = os.getenv("LEADTEH_API_TOKEN", "").strip()
LEADTEH_BOT_ID = os.getenv("LEADTEH_BOT_ID", "").strip()

if PRODAMUS_FORM_URL and not PRODAMUS_FORM_URL.endswith("/"):
    PRODAMUS_FORM_URL += "/"

DB_PATH = os.path.join(os.path.dirname(__file__), "app.db")

# ---- каталог (источник правды по цене) ----
CATALOG = {
    "GIFT_BOX_LOVED": {"name": "Подарочный бокс «Это любят Люди»", "price": 4700},
    "DUBAI_CHOCO": {"name": "Дубайский шоколад", "price": 1600},
    "KARTOSHKA": {"name": "Пирожное «Картошка»", "price": 1000},
    "CHOCO_RASPBERRY_NUTS": {"name": "Шоколад с орехом и малиной", "price": 600},
    "ASSORTI_HEART": {"name": "Шоколад конфеты ассорти «В самое сердце»", "price": 1000},
    "CHOCO_ORESHEK": {"name": "Шоколад конфеты «Орешек»", "price": 1100},
    "CHOCO_ORESHEK_CARAMEL_COOKIE": {"name": "Шоколад конфеты «Орешек с карамелью и миндальным печеньем»", "price": 1300},
}


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
    # стартовые товары (создадутся один раз)
    for sku, meta in CATALOG.items():
        cur.execute(
            """
            INSERT OR IGNORE INTO inventory (sku, name, stock, reserved)
            VALUES (?, ?, 0, 0)
            """,
            (sku, meta["name"]),
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


def _leadteh_enabled() -> bool:
    return bool(LEADTEH_API_TOKEN and LEADTEH_BOT_ID)


async def _leadteh_set_variable(client: httpx.AsyncClient, contact_id: int, name: str, value: str) -> None:
    await client.post(
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


async def send_to_leadteh(order_id: str) -> None:
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

    items_text = "; ".join(
        f"{CATALOG.get(it['sku'], {}).get('name', it['sku'])} × {it.get('qty', 1)}"
        for it in items
        if it.get("sku")
    )

    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://app.leadteh.ru/api/v1/createOrUpdateContact",
            params={"api_token": LEADTEH_API_TOKEN},
            data={
                "bot_id": LEADTEH_BOT_ID,
                "messenger": "telegram",
                "name": customer.get("name", "") or "Клиент",
                "phone": customer.get("phone", ""),
                "email": customer.get("email", ""),
                "telegram_id": str(telegram_id),
                "telegram_username": telegram_username or "",
                "address": delivery.get("pickup_point", ""),
                "tags": "Оплата прошла",
            },
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=10,
        )
        data = r.json()
        contact_id = data.get("data", {}).get("id")
        if not contact_id:
            return

        variables = [
            ("order_id", order_id),
            ("amount", str(payload.get("_amount", ""))),
            ("items", items_text),
            ("delivery_method", delivery.get("method", "")),
            ("pickup_point", delivery.get("pickup_point", "")),
            ("comment", payload.get("comment", "")),
            ("payment_status", "success"),
            ("order_created_at", str(payload.get("_created_at", ""))),
        ]

        for name, value in variables:
            await _leadteh_set_variable(client, contact_id, name, value or "")
            await asyncio.sleep(0.6)


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
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
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

    for it in order.items:
        if it.sku not in CATALOG:
            raise HTTPException(400, f"Unknown sku: {it.sku}")
        p = CATALOG[it.sku]
        products.append(
            {
                "sku": it.sku,
                "name": p["name"],
                "price": p["price"],
                "quantity": it.qty,
                "type": "goods",
            }
        )
        amount += p["price"] * it.qty

    customer_extra = (
        f"Имя: {order.customer.name}\n"
        f"Email: {order.customer.email}\n"
        f"Телефон: {order.customer.phone}\n"
        f"Доставка: {order.delivery.method}\n"
        f"Пункт выдачи: {order.delivery.pickup_point}\n"
        f"Комментарий: {order.comment or ''}"
    ).strip()

    # Для подписи products держим как list (как мы формируем)
    data_for_sign: Dict[str, Any] = {
        "do": "link",
        "sys": PRODAMUS_SYS,
        "order_id": order_uuid,            # Prodamus order_id (может быть строкой)
        "order_num": order_uuid,           # на всякий случай (у некоторых сценариев)
        "customer_phone": order.customer.phone,
        "customer_email": order.customer.email,
        "customer_extra": customer_extra,
        "products": products,
    }

    # подпись (в запросе на создание ссылки)
    data_for_sign["signature"] = prodamus_sign_unicode(data_for_sign, PRODAMUS_SECRET_KEY)

    # Payform ждёт плоский формат products[0][...]
    form_data = flatten_for_prodamus(data_for_sign)

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(PRODAMUS_FORM_URL, data=form_data)

    body = (r.text or "").strip()
    if r.status_code >= 400 or not body.startswith("http"):
        raise HTTPException(502, f"Prodamus response is not a link: status={r.status_code}, body={body[:400]}")

    payment_url = body

    con = db()
    con.execute(
        "INSERT INTO orders(order_id,status,amount,payload_json,payment_url) VALUES (?,?,?,?,?)",
        (order_uuid, "created", amount, json.dumps(order.model_dump(), ensure_ascii=False), payment_url),
    )
    con.commit()
    con.close()

    return {"order_id": order_uuid, "payment_url": payment_url, "amount": amount}

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
