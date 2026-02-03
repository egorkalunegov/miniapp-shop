export function initTelegram() {
  const tg = window.Telegram?.WebApp;
  if (!tg) return;
  tg.ready();
  tg.expand();
}

export function getInitData() {
  return window.Telegram?.WebApp?.initData || "";
}

export function getUser() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user || null;
}
