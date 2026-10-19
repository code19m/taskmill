// Applies the chosen theme before the page paints, so it never flashes. A
// page that embeds the interface can pick one with ?theme=light, dark, or
// auto; the choice is remembered.
(() => {
  const key = "taskmill-theme";
  try {
    const asked = new URLSearchParams(location.search).get("theme");
    if (asked === "light" || asked === "dark") localStorage.setItem(key, asked);
    if (asked === "auto") localStorage.removeItem(key);
    const theme = localStorage.getItem(key);
    if (theme === "light" || theme === "dark") document.documentElement.dataset.theme = theme;
  } catch {
    // Storage is blocked: follow the system's theme.
  }
})();
