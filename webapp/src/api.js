function resolveApiBase() {
  const fromEnv = import.meta.env.VITE_API_BASE?.trim();
  if (fromEnv) return fromEnv;

  const host = window.location.host;

  if (host === "localhost:5173" || host === "127.0.0.1:5173") {
    return "http://127.0.0.1:8000";
  }

  if (
    host === "miniapp-shop-one.vercel.app" ||
    host === "egorrnd.ru" ||
    host === "www.egorrnd.ru"
  ) {
    return "https://api.egorrnd.ru";
  }

  return `${window.location.protocol}//${window.location.host}`;
}

export const API_BASE = resolveApiBase();

function buildUrl(path) {
  const base = API_BASE.endsWith("/") ? API_BASE.slice(0, -1) : API_BASE;
  const url = new URL(`${base}${path}`);
  url.searchParams.set("ngrok-skip-browser-warning", "1");
  return url.toString();
}


function ngrokHeaders(extra = {}) {
  return { "ngrok-skip-browser-warning": "1", Accept: "application/json", ...extra };
}

async function request(path, options = {}) {
  try {
    const r = await fetch(buildUrl(path), options);
    if (!r.ok) throw new Error(await r.text());
    return r;
  } catch (e) {
    if (e instanceof TypeError) {
      throw new Error(`Не удается подключиться к backend: ${buildUrl(path)}`);
    }
    throw e;
  }
}

export async function createOrder(payload) {
  const r = await request("/api/orders", {
    method: "POST",
    headers: ngrokHeaders({ "Content-Type": "application/json" }),
    cache: "no-store",
    body: JSON.stringify(payload),
  });
  return await r.json();
}

export async function getInventory() {
  const r = await request("/api/inventory", {
    headers: ngrokHeaders(),
    cache: "no-store",
  });
  return await r.json();
}

export async function getProducts() {
  const r = await request("/api/products", {
    headers: ngrokHeaders(),
    cache: "no-store",
  });
  return await r.json();
}

export async function updateInventory(authBasic, items) {
  const r = await request("/api/inventory", {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Basic ${authBasic}`,
      ...ngrokHeaders(),
    },
    cache: "no-store",
    body: JSON.stringify({ items }),
  });
  return await r.json();
}

export async function syncLeadteh(authBasic) {
  const r = await request("/api/leadteh/sync", {
    method: "POST",
    headers: {
      Authorization: `Basic ${authBasic}`,
      ...ngrokHeaders(),
    },
    cache: "no-store",
  });
  return await r.json();
}

export async function syncMoySklad(authBasic) {
  const r = await request("/api/moysklad/sync", {
    method: "POST",
    headers: {
      Authorization: `Basic ${authBasic}`,
      ...ngrokHeaders(),
    },
    cache: "no-store",
  });
  return await r.json();
}

export async function pushLeadteh(authBasic) {
  const r = await request("/api/leadteh/push", {
    method: "POST",
    headers: {
      Authorization: `Basic ${authBasic}`,
      ...ngrokHeaders(),
    },
    cache: "no-store",
  });
  return await r.json();
}

export async function seedProducts(authBasic) {
  const r = await request("/api/products/seed", {
    method: "POST",
    headers: {
      Authorization: `Basic ${authBasic}`,
      ...ngrokHeaders(),
    },
    cache: "no-store",
  });
  return await r.json();
}

export async function saveProductCard(authBasic, payload) {
  const r = await request(`/api/products/${encodeURIComponent(payload.sku)}`, {
    method: "PUT",
    headers: {
      Authorization: `Basic ${authBasic}`,
      "Content-Type": "application/json",
      ...ngrokHeaders(),
    },
    cache: "no-store",
    body: JSON.stringify(payload),
  });
  return await r.json();
}

export async function uploadProductImage(authBasic, file) {
  const body = new FormData();
  body.append("file", file);
  const r = await request("/api/products/image", {
    method: "POST",
    headers: {
      Authorization: `Basic ${authBasic}`,
      "ngrok-skip-browser-warning": "1",
      Accept: "application/json",
    },
    cache: "no-store",
    body,
  });
  return await r.json();
}
