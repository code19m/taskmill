// Applies the chosen theme before the page paints, so it never flashes.
// Until someone picks Light or Dark, follow the device, also when it changes.
// A page that embeds the interface can pick one with ?theme=light, dark, or
// system. The choice is remembered where the browser allows storage, and
// holds for this page where it does not.
(() => {
  // Controls that need the scripts show from the first paint.
  document.documentElement.classList.add("js");

  const key = "taskmill-theme";
  const valid = (value) => value === "light" || value === "dark";
  const asked = new URLSearchParams(location.search).get("theme");
  let choice = valid(asked) ? asked : "system";
  try {
    if (valid(asked)) localStorage.setItem(key, asked);
    if (asked === "system" || asked === "auto") localStorage.removeItem(key);
    const saved = localStorage.getItem(key);
    if (valid(saved)) choice = saved;
  } catch {
    // Storage is blocked, as it can be in another site's frame: the address
    // decides for this page, and nothing is remembered.
  }
  let device = null;
  try { device = matchMedia("(prefers-color-scheme: dark)"); } catch { device = null; }
  const resolved = () => (choice !== "system" ? choice : device?.matches === false ? "light" : "dark");

  const apply = () => {
    const theme = resolved();
    document.documentElement.dataset.theme = theme;
    const meta = document.querySelector('meta[name="theme-color"]');
    if (meta) meta.content = theme === "light" ? "#ffffff" : "#181818";
    for (const button of document.querySelectorAll("[data-theme-option]")) {
      button.setAttribute("aria-pressed", String(button.dataset.themeOption === choice));
    }
  };

  window.TaskmillTheme = {
    get choice() { return choice; },
    set(value) {
      if (!valid(value) && value !== "system") return;
      choice = value;
      try {
        if (value === "system") localStorage.removeItem(key);
        else localStorage.setItem(key, value);
      } catch { /* not remembered */ }
      // Switch every color at once, not some over a transition.
      const root = document.documentElement;
      root.classList.add("switching");
      apply();
      requestAnimationFrame(() => requestAnimationFrame(() => root.classList.remove("switching")));
    },
    apply,
  };
  try { device?.addEventListener("change", () => { if (choice === "system") apply(); }); } catch { /* old browser */ }
  apply();
})();
