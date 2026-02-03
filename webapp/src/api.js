export const API_BASE =
  import.meta.env.VITE_API_BASE?.trim() ||
  `${window.location.protocol}//${window.location.host}`;

function buildUrl(path) {
  const base = API_BASE.endsWith("/") ? API_BASE.slice(0, -1) : API_BASE;
  const url = new URL(`${base}${path}`);
  url.searchParams.set("ngrok-skip-browser-warning", "1");
  return url.toString();
}


function ngrokHeaders(extra = {}) {
  return { "ngrok-skip-browser-warning": "1", ...extra };
}

export async function createOrder(payload) {
  const r = await fetch(buildUrl("/api/orders"), {
    method: "POST",
    headers: ngrokHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(payload),
  });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function getInventory() {
  const r = await fetch(buildUrl("/api/inventory"), {
    headers: ngrokHeaders(),
  });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function updateInventory(authBasic, items) {
  const r = await fetch(buildUrl("/api/inventory"), {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Basic ${authBasic}`,
      ...ngrokHeaders(),
    },
    body: JSON.stringify({ items }),
  });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}
