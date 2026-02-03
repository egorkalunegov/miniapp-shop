export const API_BASE = "http://127.0.0.1:8000";


export async function createOrder(payload) {
  const r = await fetch(`${API_BASE}/api/orders`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function getInventory() {
  const r = await fetch(`${API_BASE}/api/inventory`);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function updateInventory(authBasic, items) {
  const r = await fetch(`${API_BASE}/api/inventory`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Basic ${authBasic}`,
    },
    body: JSON.stringify({ items }),
  });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}
