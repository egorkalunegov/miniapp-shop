import React, { useEffect, useMemo, useState } from "react";
import {
  API_BASE,
  createOrder,
  getProducts,
  pushLeadteh,
  saveProductCard,
  seedProducts,
  syncLeadteh,
  syncMoySklad,
  updateInventory,
  uploadProductImage,
} from "./api.js";
import { getInitData, getMessengerPlatform, getUser, initMessenger, openExternalLink } from "./telegram.js";

function rub(v) {
  return new Intl.NumberFormat("ru-RU").format(v) + " ₽";
}

function normalizePhoneForSend(value) {
  const digits = String(value || "").replace(/\D+/g, "");
  if (!digits) return "";
  if (digits.length === 11 && digits.startsWith("8")) return `+7${digits.slice(1)}`;
  if (digits.length === 11 && digits.startsWith("7")) return `+${digits}`;
  if (digits.length === 10) return `+7${digits}`;
  return value;
}

function emptyAdminCardDraft() {
  return {
    sku: "",
    name: "",
    price: 0,
    weight: "",
    shelfLife: "",
    description: "",
    imageUrl: "",
    badge: "",
    sort: 0,
    active: 1,
  };
}

function productToAdminCardDraft(product) {
  return {
    sku: product?.sku || "",
    name: product?.name || "",
    price: Number(product?.price || 0),
    weight: product?.weight || "",
    shelfLife: product?.shelfLife || "",
    description: product?.description || "",
    imageUrl: product?.imageUrl || "",
    badge: product?.badge || "",
    sort: Number(product?.sort || 0),
    active: product?.active === 0 ? 0 : 1,
  };
}

export default function App() {
  const [cart, setCart] = useState({});
  const [step, setStep] = useState("shop"); // shop|checkout|done
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [activeSku, setActiveSku] = useState("");
  const [products, setProducts] = useState([]);
  const [productsError, setProductsError] = useState("");
  const [adminUser, setAdminUser] = useState("");
  const [adminPass, setAdminPass] = useState("");
  const [adminAuth, setAdminAuth] = useState("");
  const [adminStocks, setAdminStocks] = useState({});
  const [adminSaving, setAdminSaving] = useState(false);
  const [adminError, setAdminError] = useState("");
  const [adminSyncing, setAdminSyncing] = useState(false);
  const [adminMoySkladSyncing, setAdminMoySkladSyncing] = useState(false);
  const [adminSyncMsg, setAdminSyncMsg] = useState("");
  const [adminPushing, setAdminPushing] = useState(false);
  const [adminPushMsg, setAdminPushMsg] = useState("");
  const [adminSeeding, setAdminSeeding] = useState(false);
  const [adminCardDraft, setAdminCardDraft] = useState(emptyAdminCardDraft);
  const [adminCardSaving, setAdminCardSaving] = useState(false);
  const [adminCardUploading, setAdminCardUploading] = useState(false);
  const [adminCardMsg, setAdminCardMsg] = useState("");
  const [adminCardMode, setAdminCardMode] = useState("edit");
  const [adminCardOpen, setAdminCardOpen] = useState(false);
  const [lastOrder, setLastOrder] = useState(null);

  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [phone, setPhone] = useState("");
  const [delivery, setDelivery] = useState("cdek");
  const [pickupPoint, setPickupPoint] = useState("");
  const [comment, setComment] = useState("");

  const messengerUser = useMemo(() => getUser(), []);
  const messengerPlatform = useMemo(() => getMessengerPlatform(), []);
  const adminRoute = useMemo(() => {
    const ADMIN_HASH = "#/peopleloveit-admin";
    return window.location.hash === ADMIN_HASH;
  }, []);

  useEffect(() => {
    initMessenger();
    if (messengerUser?.first_name && !name) setName(messengerUser.first_name);
  }, []);

  const loadProducts = () =>
    getProducts()
      .then((rows) => {
        setProducts(rows || []);
        setProductsError("");
      })
      .catch((e) => setProductsError(String(e?.message || e)));

  useEffect(() => {
    loadProducts();
  }, [adminRoute]);

  useEffect(() => {
    if (!adminRoute) return;
    const stocks = {};
    products.forEach((p) => {
      stocks[p.sku] = p.stock ?? 0;
    });
    setAdminStocks(stocks);
  }, [adminRoute, products]);

  const productMap = useMemo(() => {
    const map = {};
    products.forEach((p) => {
      map[p.sku] = p;
    });
    return map;
  }, [products]);

  const items = useMemo(
    () =>
      Object.entries(cart)
        .filter(([sku, qty]) => qty > 0 && productMap[sku])
        .map(([sku, qty]) => ({ sku, qty })),
    [cart, productMap]
  );

  const visibleProducts = useMemo(
    () => products.filter((p) => p.active !== 0),
    [products]
  );

  const catalogSorted = useMemo(() => {
    const arr = [...visibleProducts];
    arr.sort((a, b) => {
      const aAvail = a.available;
      const bAvail = b.available;
      const aOut = aAvail !== undefined ? aAvail <= 0 : false;
      const bOut = bAvail !== undefined ? bAvail <= 0 : false;
      if (aOut !== bOut) return aOut ? 1 : -1;
      const aSort = a.sort ?? 0;
      const bSort = b.sort ?? 0;
      if (aSort !== bSort) return aSort - bSort;
      return String(a.name).localeCompare(String(b.name), "ru");
    });
    return arr;
  }, [visibleProducts]);

  const cartCount = useMemo(() => items.reduce((a, b) => a + b.qty, 0), [items]);
  const hasMotiInCart = useMemo(
    () => items.some((it) => it.sku === "MOTI_Coockies"),
    [items]
  );

  const total = useMemo(() => {
    let s = 0;
    for (const it of items) {
      const p = productMap[it.sku];
      if (p) s += Number(p.price || 0) * it.qty;
    }
    return s;
  }, [items, productMap]);

  const deliveryFee = useMemo(() => {
    if (delivery === "ozon") return 200;
    return 0;
  }, [delivery]);

  const totalWithDelivery = total + deliveryFee;

  const inc = (sku) => setCart((c) => ({ ...c, [sku]: (c[sku] || 0) + 1 }));
  const dec = (sku) =>
    setCart((c) => ({ ...c, [sku]: Math.max(0, (c[sku] || 0) - 1) }));

  const activeProduct = useMemo(
    () => productMap[activeSku] || null,
    [activeSku, productMap]
  );

  function validatePhone(value) {
    const normalized = normalizePhoneForSend(value);
    const digits = normalized.replace(/\D+/g, "");
    return digits.length === 11 && digits.startsWith("7");
  }

  function openPayment(url) {
    if (!url) return;
    openExternalLink(url);
  }

  async function submit() {
    setError("");
    if (!items.length) return setError("Корзина пустая.");
    if (!name.trim()) return setError("Введите имя.");
    if (!email.trim()) return setError("Введите email.");
    if (!phone.trim()) return setError("Введите телефон.");
    if (!validatePhone(phone.trim())) return setError("Введите телефон в формате +7 900 000-00-00.");
    if (!pickupPoint.trim()) return setError("Укажите пункт выдачи.");

    setLoading(true);
    try {
      const payload = {
        initData: getInitData(),
        messenger_platform: messengerPlatform !== "web" ? messengerPlatform : null,
        messenger_user_id: messengerUser?.id ? String(messengerUser.id) : null,
        messenger_username: messengerUser?.username || null,
        telegram_id: messengerPlatform === "telegram" ? messengerUser?.id || null : null,
        telegram_username: messengerPlatform === "telegram" ? messengerUser?.username || null : null,
        customer: {
          name: name.trim(),
          email: email.trim(),
          phone: normalizePhoneForSend(phone.trim()),
        },
        delivery: { method: delivery, pickup_point: pickupPoint.trim() },
        comment: comment.trim(),
        items
      };

      const res = await createOrder(payload);
      const payUrl = res.payment_url_direct || res.payment_url;

      const itemsSnapshot = items.map((it) => {
        const p = productMap[it.sku] || {};
        return {
          sku: it.sku,
          qty: it.qty,
          name: p.name || it.sku,
          price: Number(p.price || 0),
        };
      });
      const itemsTotal = itemsSnapshot.reduce((s, it) => s + it.price * it.qty, 0);
      setLastOrder({
        order_id: res.order_id,
        payment_url: payUrl,
        amount: Number(res.amount || 0),
        items: itemsSnapshot,
        items_total: itemsTotal,
        delivery_fee: deliveryFee,
        total_with_delivery: itemsTotal + deliveryFee,
        customer: payload.customer,
        delivery: payload.delivery,
        comment: payload.comment,
        messenger_platform: payload.messenger_platform,
        messenger_user_id: payload.messenger_user_id,
        messenger_username: payload.messenger_username,
      });

      openPayment(payUrl);
      setStep("done");
    } catch (e) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function adminLogin(e) {
    e.preventDefault();
    setAdminError("");
    const token = btoa(`${adminUser}:${adminPass}`);
    try {
      await updateInventory(token, []);
      setAdminAuth(token);
    } catch (e) {
      const msg = String(e?.message || e);
      if (msg.includes("Invalid credentials") || msg.includes("401")) {
        setAdminError("Неверный логин или пароль.");
      } else {
        setAdminError(`Не удается подключиться к backend (${msg})`);
      }
    }
  }

  function startCreateCard() {
    setAdminCardMode("create");
    setAdminCardMsg("");
    setAdminCardDraft(emptyAdminCardDraft());
    setAdminCardOpen(true);
  }

  function startEditCard(product) {
    setAdminCardMode("edit");
    setAdminCardMsg("");
    setAdminCardDraft(productToAdminCardDraft(product));
    setAdminCardOpen(true);
  }

  function closeAdminCard() {
    setAdminCardOpen(false);
    setAdminCardMsg("");
  }

  function updateAdminCardField(field, value) {
    setAdminCardDraft((current) => ({ ...current, [field]: value }));
  }

  async function handleAdminImageUpload(file) {
    if (!file || !adminAuth) return;
    setAdminError("");
    setAdminCardMsg("");
    setAdminCardUploading(true);
    try {
      const res = await uploadProductImage(adminAuth, file);
      setAdminCardDraft((current) => ({ ...current, imageUrl: res.imageUrl || "" }));
      setAdminCardMsg("Картинка загружена.");
    } catch (e) {
      setAdminError(String(e?.message || e));
    } finally {
      setAdminCardUploading(false);
    }
  }

  async function saveAdminCard() {
    if (!adminAuth) return;
    if (!adminCardDraft.sku.trim()) return setAdminError("Укажите SKU.");
    if (!adminCardDraft.name.trim()) return setAdminError("Укажите название.");
    setAdminError("");
    setAdminCardMsg("");
    setAdminCardSaving(true);
    try {
      const payload = {
        ...adminCardDraft,
        sku: adminCardDraft.sku.trim(),
        name: adminCardDraft.name.trim(),
        price: Number(adminCardDraft.price || 0),
        sort: Number(adminCardDraft.sort || 0),
        active: Number(adminCardDraft.active || 0),
      };
      await saveProductCard(adminAuth, payload);
      await loadProducts();
      setAdminCardMode("edit");
      setAdminCardMsg("Карточка сохранена.");
      setAdminCardOpen(true);
    } catch (e) {
      setAdminError(String(e?.message || e));
    } finally {
      setAdminCardSaving(false);
    }
  }

  async function saveInventory() {
    setAdminError("");
    setAdminSaving(true);
    try {
      const itemsToSave = products.map((p) => ({
        sku: p.sku,
        stock: Number(adminStocks[p.sku] ?? 0),
      }));
      await updateInventory(adminAuth, itemsToSave);
      await loadProducts();
    } catch (e) {
      setAdminError(String(e?.message || e));
    } finally {
      setAdminSaving(false);
    }
  }

  async function syncFromLeadteh() {
    setAdminError("");
    setAdminSyncMsg("");
    setAdminSyncing(true);
    try {
      const res = await syncLeadteh(adminAuth);
      setAdminSyncMsg(`Синхронизировано: ${res.created || 0} новых, ${res.updated || 0} обновлено, ${res.skipped || 0} пропущено.`);
      await loadProducts();
    } catch (e) {
      setAdminError(String(e?.message || e));
    } finally {
      setAdminSyncing(false);
    }
  }

  async function syncFromMoySklad() {
    setAdminError("");
    setAdminSyncMsg("");
    setAdminMoySkladSyncing(true);
    try {
      const res = await syncMoySklad(adminAuth);
      setAdminSyncMsg(`МойСклад: ${res.created || 0} новых, ${res.updated || 0} обновлено, ${res.skipped || 0} пропущено.`);
      await loadProducts();
    } catch (e) {
      setAdminError(String(e?.message || e));
    } finally {
      setAdminMoySkladSyncing(false);
    }
  }

  async function pushToLeadteh() {
    setAdminError("");
    setAdminPushMsg("");
    setAdminPushing(true);
    try {
      const res = await pushLeadteh(adminAuth);
      setAdminPushMsg(`Выгружено в Leadteh: ${res.created || 0} новых, ${res.updated || 0} обновлено.`);
    } catch (e) {
      setAdminError(String(e?.message || e));
    } finally {
      setAdminPushing(false);
    }
  }

  async function seedFromTemplate() {
    setAdminError("");
    setAdminSyncMsg("");
    setAdminSeeding(true);
    try {
      await seedProducts(adminAuth);
      setAdminSyncMsg("Карточки восстановлены из шаблона.");
      await loadProducts();
    } catch (e) {
      setAdminError(String(e?.message || e));
    } finally {
      setAdminSeeding(false);
    }
  }

  if (adminRoute) {
    return (
      <div className="wrap">
        <header className="top">
          <div className="title">Админка</div>
          <a className="adminLink" href="/">В магазин</a>
        </header>

        {!adminAuth && (
          <form className="box" onSubmit={adminLogin}>
            <div className="boxTitle">Вход</div>
            <div className="muted">Backend: {API_BASE}</div>
            <label className="field">
              <span>Логин</span>
              <input value={adminUser} onChange={(e) => setAdminUser(e.target.value)} />
            </label>
            <label className="field">
              <span>Пароль</span>
              <input type="password" value={adminPass} onChange={(e) => setAdminPass(e.target.value)} />
            </label>
            {adminError && <div className="error">{adminError}</div>}
            <button className="payBtn" type="submit">Войти</button>
          </form>
        )}

        {adminAuth && (
          <div className="box">
            <div className="boxTitle">Карточки и остатки</div>
            {adminError && <div className="error">{adminError}</div>}
            {adminSyncMsg && <div className="muted">{adminSyncMsg}</div>}
            {adminCardMsg && <div className="muted">{adminCardMsg}</div>}
            <div className="muted">
              Карточки теперь можно править прямо здесь. Остатки продолжают жить отдельно: МойСклад и Leadteh обновляют stock,
              а локально сохраненные карточки не перетираются синком.
            </div>
            <div className="adminActions">
              <button type="button" className="openBtn" onClick={syncFromMoySklad} disabled={adminMoySkladSyncing}>
                {adminMoySkladSyncing ? "Синхронизация..." : "Синхронизировать из МойСклад"}
              </button>
              <button type="button" className="openBtn" onClick={syncFromLeadteh} disabled={adminSyncing}>
                {adminSyncing ? "Синхронизация..." : "Синхронизировать из Leadteh"}
              </button>
              <button type="button" className="openBtn" onClick={pushToLeadteh} disabled={adminPushing}>
                {adminPushing ? "Выгрузка..." : "Выгрузить в Leadteh"}
              </button>
              <button type="button" className="openBtn" onClick={seedFromTemplate} disabled={adminSeeding}>
                {adminSeeding ? "Восстановление..." : "Восстановить карточки"}
              </button>
              <button type="button" className="openBtn" onClick={startCreateCard}>
                Добавить карточку
              </button>
            </div>
            {adminPushMsg && <div className="muted">{adminPushMsg}</div>}

            {adminCardOpen && (
            <div className="adminEditor">
              <div className="adminEditorTop">
                <div>
                  <div className="boxTitle">{adminCardMode === "create" ? "Новая карточка" : "Редактирование карточки"}</div>
                  <div className="muted">
                    Сохраняется локальная карточка. Остатки дальше можно синхронизировать отдельно.
                  </div>
                </div>
                <div className="adminEditorAside">
                  {adminCardDraft.imageUrl && (
                    <img className="adminPreview" src={adminCardDraft.imageUrl} alt={adminCardDraft.name || "preview"} />
                  )}
                  <button type="button" className="openBtn" onClick={closeAdminCard}>
                    Закрыть
                  </button>
                </div>
              </div>

              <div className="adminGrid">
                <label className="field">
                  <span>SKU</span>
                  <input
                    value={adminCardDraft.sku}
                    onChange={(e) => updateAdminCardField("sku", e.target.value)}
                    disabled={adminCardMode === "edit"}
                  />
                </label>
                <label className="field">
                  <span>Название</span>
                  <input value={adminCardDraft.name} onChange={(e) => updateAdminCardField("name", e.target.value)} />
                </label>
                <label className="field">
                  <span>Цена</span>
                  <input
                    type="number"
                    min="0"
                    value={adminCardDraft.price}
                    onChange={(e) => updateAdminCardField("price", e.target.value)}
                  />
                </label>
                <label className="field">
                  <span>Порядок</span>
                  <input
                    type="number"
                    value={adminCardDraft.sort}
                    onChange={(e) => updateAdminCardField("sort", e.target.value)}
                  />
                </label>
                <label className="field">
                  <span>Вес</span>
                  <input value={adminCardDraft.weight} onChange={(e) => updateAdminCardField("weight", e.target.value)} />
                </label>
                <label className="field">
                  <span>Срок годности</span>
                  <input
                    value={adminCardDraft.shelfLife}
                    onChange={(e) => updateAdminCardField("shelfLife", e.target.value)}
                  />
                </label>
                <label className="field">
                  <span>Бейдж</span>
                  <input value={adminCardDraft.badge} onChange={(e) => updateAdminCardField("badge", e.target.value)} />
                </label>
                <label className="field">
                  <span>Активность</span>
                  <select
                    value={String(adminCardDraft.active)}
                    onChange={(e) => updateAdminCardField("active", Number(e.target.value))}
                  >
                    <option value="1">Показывать</option>
                    <option value="0">Скрыть</option>
                  </select>
                </label>
              </div>

              <label className="field">
                <span>Описание</span>
                <textarea
                  value={adminCardDraft.description}
                  onChange={(e) => updateAdminCardField("description", e.target.value)}
                />
              </label>

              <label className="field">
                <span>URL картинки</span>
                <input
                  value={adminCardDraft.imageUrl}
                  onChange={(e) => updateAdminCardField("imageUrl", e.target.value)}
                />
              </label>

              <label className="field">
                <span>Загрузить новую картинку</span>
                <input
                  type="file"
                  accept="image/*"
                  onChange={(e) => handleAdminImageUpload(e.target.files?.[0])}
                />
              </label>

              <div className="adminActions">
                <button type="button" className="payBtn adminSaveBtn" onClick={saveAdminCard} disabled={adminCardSaving || adminCardUploading}>
                  {adminCardSaving ? "Сохраняем карточку..." : "Сохранить карточку"}
                </button>
                <button
                  type="button"
                  className="openBtn"
                  onClick={() => (adminCardMode === "create" ? setAdminCardDraft(emptyAdminCardDraft()) : closeAdminCard())}
                >
                  {adminCardMode === "create" ? "Очистить форму" : "Закрыть"}
                </button>
              </div>
              {adminCardUploading && <div className="muted">Загрузка картинки...</div>}
            </div>
            )}

            <div className="adminList">
              {products.map((p) => (
                <div className="adminRow" key={p.sku}>
                  <div className="adminRowMain">
                    {p.imageUrl ? <img className="adminThumb" src={p.imageUrl} alt={p.name} /> : <div className="adminThumb adminThumb--empty" />}
                    <div>
                      <div className="adminName">{p.name}</div>
                      <div className="muted">SKU: {p.sku}</div>
                      <div className="muted">
                        {rub(Number(p.price || 0))} · Остаток {p.stock ?? 0} · {p.catalogOverride ? "локальная карточка" : "внешняя карточка"}
                      </div>
                    </div>
                  </div>
                  <div className="adminRowSide">
                    <input
                      className="adminInput"
                      type="number"
                      min="0"
                      value={adminStocks[p.sku] ?? 0}
                      onChange={(e) =>
                        setAdminStocks((s) => ({ ...s, [p.sku]: e.target.value }))
                      }
                    />
                    <button type="button" className="openBtn" onClick={() => startEditCard(p)}>
                      Редактировать
                    </button>
                  </div>
                </div>
              ))}
            </div>
            <button className="payBtn" onClick={saveInventory} disabled={adminSaving}>
              {adminSaving ? "Сохраняем..." : "Сохранить"}
            </button>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="wrap">
      <header className="top">
        <div>
          <div className="title titleRow">
            <img className="logo" src="/peopleloveit.jpg" alt="Люди это любят" />
            <span>ЭТО ЛЮБЯТ ЛЮДИ</span>
          </div>
        </div>
        {step !== "checkout" && (
          <button
            className="cartBtn cartBtn--sticky"
            onClick={() => setStep(step === "checkout" ? "shop" : "checkout")}
          >
            🧺 Корзина ({cartCount})
          </button>
        )}
      </header>
      {productsError && <div className="error">{productsError}</div>}

      {step === "shop" && (
        <div className="grid">
          {catalogSorted.map((p) => {
            const qty = cart[p.sku] || 0;
            const available = p.available;
            const isOut = available !== undefined && available <= 0;
            const isMotiProduct = p.sku === "MOTI_Coockies";
            return (
              <div key={p.sku} className={`card ${isOut ? "card--out" : ""}`}>
                <div className="media">
                  <img className="img" src={p.imageUrl} alt={p.name} />
                  {p.badge && <div className="badge">{p.badge}</div>}
                  {isOut && <div className="badge badge--out">Нет в наличии</div>}
                  <div className="priceTag">{rub(p.price)}</div>
                </div>
                <div className="cardBody">
                  {isMotiProduct && (
                    <div className="infoStrip">Продажа по Ростову-на-Дону и ДНР</div>
                  )}
                  <div className="cardTitle">{p.name}</div>
                  <div className="chips">
                    {p.weight && <span className="chip">{p.weight}</span>}
                    {p.shelfLife && <span className="chip">Годен: {p.shelfLife}</span>}
                  </div>
                  <div className={`desc ${p.sku === "GIFT_BOX_LOVED" ? "descLines" : ""}`}>
                    {p.description}
                  </div>
                  <div className="cardFooter">
                    <div className="priceHint">Цена за шт.</div>
                    {qty === 0 ? (
                      <button className="addBtn" onClick={() => inc(p.sku)} disabled={isOut}>
                        Добавить
                      </button>
                    ) : (
                      <div className="qty">
                        <button className="btn" onClick={() => dec(p.sku)}>−</button>
                        <div className="qtyNum">{qty}</div>
                        <button className="btn" onClick={() => inc(p.sku)} disabled={isOut}>+</button>
                      </div>
                    )}
                  </div>
                  <button
                    className="openBtn"
                    onClick={() => setActiveSku(p.sku)}
                  >
                    Подробнее
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {step === "checkout" && (
        <div className="checkout">
          <div className="checkoutTop">
            <h2>Оформление заказа</h2>
            <button className="openBtn" onClick={() => setStep("shop")}>
              К ТОВАРАМ
            </button>
          </div>

          {hasMotiInCart && (
            <div className="warningBanner">
              Продажа моти осуществляется только по Ростову-на-Дону и ДНР
            </div>
          )}

          <div className="box">
            <div className="boxTitle">Корзина</div>
            {items.length === 0 ? (
              <div className="muted">Пока пусто</div>
            ) : (
              <div>
                {items.map((it) => {
                  const p = productMap[it.sku];
                  return (
                    <div key={it.sku} className="cartRow">
                      <div>
                        <div style={{ fontWeight: 800 }}>{p?.name}</div>
                        <div className="muted">{it.qty} × {rub(p?.price || 0)}</div>
                      </div>
                      <div className="qty">
                        <button className="btn" onClick={() => dec(it.sku)}>−</button>
                        <div className="qtyNum">{it.qty}</div>
                        <button className="btn" onClick={() => inc(it.sku)}>+</button>
                      </div>
                    </div>
                  );
                })}
                <div className="cartRow">
                  <div className="muted">Товар</div>
                  <div>{rub(total)}</div>
                </div>
                {deliveryFee > 0 && (
                  <div className="cartRow">
                    <div className="muted">Доставка</div>
                    <div>+ {rub(deliveryFee)}</div>
                  </div>
                )}
                <div className="total">Итого: <b>{rub(totalWithDelivery)}</b></div>
              </div>
            )}
          </div>

          <div className="box">
            <div className="boxTitle">Данные</div>

            <label className="field">
              <span>Ваше имя</span>
              <input value={name} onChange={(e) => setName(e.target.value)} />
            </label>

            <label className="field">
              <span>Электронная почта</span>
              <input value={email} onChange={(e) => setEmail(e.target.value)} />
            </label>

            <label className="field">
              <span>Номер телефона</span>
              <input
                value={phone}
                onChange={(e) => setPhone(e.target.value)}
                onBlur={(e) => setPhone(normalizePhoneForSend(e.target.value))}
                placeholder="+7 900 000-00-00"
                inputMode="tel"
              />
              <div className="muted">Формат: +7 900 000-00-00 (если введёте 8..., заменим на +7)</div>
            </label>

            <label className="field">
              <span>Доставка</span>
              <select value={delivery} onChange={(e) => setDelivery(e.target.value)}>
                <option value="cdek">CDEK</option>
                <option value="ozon">Ozon</option>
                <option value="wildberries">Wildberries</option>
              </select>
              {delivery === "cdek" && (
                <div className="muted">
                  Доставка СДЭК (согласование с менеджером), оплачивается отдельно в день получения заказа.
                </div>
              )}
              {delivery === "wildberries" && (
                <div className="muted">
                  Доставка Wildbirries (согласование с менеджером), оплачивается отдельно в день получения заказа.
                </div>
              )}
            </label>

            <label className="field">
              <span>Пункт выдачи (адрес/код)</span>
              <input value={pickupPoint} onChange={(e) => setPickupPoint(e.target.value)} />
              <div className="muted">
                *За некачественную доставку транспортной компании (СДЭК, OZON, WB) ответственности не несем.
              </div>
            </label>

            <label className="field">
              <span>Комментарий (необязательно)</span>
              <textarea value={comment} onChange={(e) => setComment(e.target.value)} />
            </label>

            {error && <div className="error">{error}</div>}

            <button className="payBtn" onClick={submit} disabled={loading || items.length === 0}>
              {loading ? "Создаём ссылку оплаты..." : `Перейти к оплате • ${rub(totalWithDelivery)}`}
            </button>
          </div>
        </div>
      )}

      {step === "done" && (
        <div className="checkout">
          <div className="box">
            <div className="boxTitle">Заказ оформлен ✅</div>
            <div className="muted">Ссылка на оплату открыта. Ниже детали заказа.</div>

            {!lastOrder && (
              <div className="muted" style={{ marginTop: 10 }}>
                Нет данных заказа. Вернитесь в магазин и оформите заново.
              </div>
            )}

            {lastOrder && (
              <>
                <div className="cartRow" style={{ marginTop: 10 }}>
                  <div className="muted">Номер заказа</div>
                  <div>{lastOrder.order_id}</div>
                </div>

                <div className="boxTitle" style={{ marginTop: 12 }}>Товары</div>
                {lastOrder.items.map((it) => (
                  <div key={it.sku} className="cartRow">
                    <div>
                      <div style={{ fontWeight: 800 }}>{it.name}</div>
                      <div className="muted">{it.qty} × {rub(it.price)}</div>
                    </div>
                    <div>{rub(it.price * it.qty)}</div>
                  </div>
                ))}
                <div className="cartRow">
                  <div className="muted">Сумма товаров</div>
                  <div>{rub(lastOrder.items_total)}</div>
                </div>
                {lastOrder.delivery_fee > 0 && (
                  <div className="cartRow">
                    <div className="muted">Доставка</div>
                    <div>+ {rub(lastOrder.delivery_fee)}</div>
                  </div>
                )}
                <div className="total">Итого: <b>{rub(lastOrder.total_with_delivery)}</b></div>

                <div className="boxTitle" style={{ marginTop: 12 }}>Контакты</div>
                <div className="cartRow">
                  <div className="muted">Имя</div>
                  <div>{lastOrder.customer?.name}</div>
                </div>
                <div className="cartRow">
                  <div className="muted">Email</div>
                  <div>{lastOrder.customer?.email}</div>
                </div>
                <div className="cartRow">
                  <div className="muted">Телефон</div>
                  <div>{lastOrder.customer?.phone}</div>
                </div>
                <div className="cartRow">
                  <div className="muted">Мессенджер</div>
                  <div>
                    {lastOrder.messenger_platform || "web"}
                    {" • "}
                    {lastOrder.messenger_username
                      ? `@${lastOrder.messenger_username}`
                      : (lastOrder.messenger_user_id || "—")}
                    {lastOrder.messenger_username && lastOrder.messenger_user_id
                      ? ` (id ${lastOrder.messenger_user_id})`
                      : ""}
                  </div>
                </div>

                <div className="boxTitle" style={{ marginTop: 12 }}>Доставка</div>
                <div className="cartRow">
                  <div className="muted">Способ</div>
                  <div>{lastOrder.delivery?.method}</div>
                </div>
                <div className="cartRow">
                  <div className="muted">Пункт выдачи</div>
                  <div>{lastOrder.delivery?.pickup_point}</div>
                </div>
                {lastOrder.comment && (
                  <div className="cartRow">
                    <div className="muted">Комментарий</div>
                    <div>{lastOrder.comment}</div>
                  </div>
                )}
              </>
            )}

            <div style={{ display: "grid", gap: 8, marginTop: 12 }}>
              {lastOrder?.payment_url && (
                <button className="openBtn" onClick={() => openPayment(lastOrder.payment_url)}>
                  Открыть оплату еще раз
                </button>
              )}
              <button className="cartBtn" onClick={() => setStep("shop")}>
                Вернуться в магазин
              </button>
            </div>
          </div>
        </div>
      )}

      {activeProduct && (
        <div className="modal" role="dialog" aria-modal="true">
          <div className="modalBackdrop" onClick={() => setActiveSku("")} />
          <div className="modalCard">
            <button className="closeBtn" onClick={() => setActiveSku("")}>✕</button>
            <img className="modalImg" src={activeProduct.imageUrl} alt={activeProduct.name} />
            <div className="modalBody">
              {activeProduct.sku === "MOTI_Coockies" && (
                <div className="infoStrip">Продажа по Ростову-на-Дону и ДНР</div>
              )}
              <div className="modalTitle">{activeProduct.name}</div>
              <div className="modalMeta">
                {activeProduct.weight && <span className="chip">{activeProduct.weight}</span>}
                {activeProduct.shelfLife && <span className="chip">Годен: {activeProduct.shelfLife}</span>}
                <span className="chip">{rub(activeProduct.price)}</span>
              </div>
              <div className={`desc ${activeProduct.sku === "GIFT_BOX_LOVED" ? "descLines" : ""}`}>
                {activeProduct.description}
              </div>
              <div className="modalActions">
                {(() => {
                  const available = activeProduct.available;
                  const isOut = available !== undefined && available <= 0;
                  if ((cart[activeProduct.sku] || 0) === 0) {
                    return (
                      <button className="addBtn" onClick={() => inc(activeProduct.sku)} disabled={isOut}>
                        Добавить в корзину
                      </button>
                    );
                  }
                  return (
                    <div className="qty">
                      <button className="btn" onClick={() => dec(activeProduct.sku)}>−</button>
                      <div className="qtyNum">{cart[activeProduct.sku]}</div>
                      <button className="btn" onClick={() => inc(activeProduct.sku)} disabled={isOut}>+</button>
                    </div>
                  );
                })()}
                <button className="ghostBtn" onClick={() => setActiveSku("")}>
                  Закрыть
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
