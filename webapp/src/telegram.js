function getTelegramApp() {
  return window.Telegram?.WebApp || null;
}

function getMaxApp() {
  return window.WebApp || null;
}

export function getMessengerPlatform() {
  if (getTelegramApp()) return "telegram";
  if (getMaxApp()) return "max";
  return "web";
}

export function getMessengerApp() {
  return getTelegramApp() || getMaxApp();
}

export function initMessenger() {
  const app = getMessengerApp();
  if (!app) return;
  if (typeof app.ready === "function") app.ready();
  if (typeof app.expand === "function") app.expand();
}

export function getInitData() {
  return getMessengerApp()?.initData || "";
}

export function getUser() {
  return getMessengerApp()?.initDataUnsafe?.user || null;
}

export function openExternalLink(url) {
  const app = getMessengerApp();
  if (app && typeof app.openLink === "function") {
    app.openLink(url);
    return;
  }
  window.location.href = url;
}
