// The interface works without this script. It adds live updates, a
// confirmation dialog, a chart of the counts, payload highlighting, and a few
// conveniences.
(() => {
  "use strict";

  const REFRESH_MS = 5000;
  const CHART_WINDOW_MS = 10 * 60 * 1000;
  const KEYS = { live: "taskmill-live", samples: "taskmill-samples" };

  const store = (area) => ({
    get(key) {
      try { return area().getItem(key); } catch { return null; }
    },
    set(key, value) {
      try {
        if (value == null) area().removeItem(key);
        else area().setItem(key, value);
      } catch { /* storage is blocked: nothing is remembered */ }
    },
  });
  const local = store(() => localStorage);
  const session = store(() => sessionStorage);

  // Controls that need this script stay hidden without it.

  for (const control of document.querySelectorAll("[data-live-toggle], .theme-seg, [data-copy]")) {
    control.hidden = false;
  }

  // Theme: System, Light, or Dark

  document.addEventListener("click", (event) => {
    const option = event.target.closest("[data-theme-option]");
    if (option) window.TaskmillTheme?.set(option.dataset.themeOption);
  });
  window.TaskmillTheme?.apply();

  // Confirmation dialog

  const dialog = document.getElementById("confirm-dialog");
  let confirming = null;

  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement) || !form.dataset.confirm) return;
    event.preventDefault();
    if (!dialog || typeof dialog.showModal !== "function") {
      if (window.confirm(`${form.dataset.confirmTitle || ""} ${form.dataset.confirm}`.trim())) form.submit();
      return;
    }
    confirming = form;
    const danger = "confirmDanger" in form.dataset;
    const button = dialog.querySelector("[data-confirm-button]");
    dialog.querySelector("h2").textContent = form.dataset.confirmTitle || "Are you sure?";
    dialog.querySelector("[data-confirm-text]").textContent = form.dataset.confirm;
    button.textContent = form.dataset.confirmLabel || "Confirm";
    button.classList.toggle("primary", !danger);
    button.classList.toggle("danger-solid", danger);
    dialog.showModal();
  });

  dialog?.addEventListener("close", () => {
    // submit() skips the submit event, so the dialog does not open again.
    if (dialog.returnValue === "confirm" && confirming) confirming.submit();
    confirming = null;
  });

  // Rows that lead somewhere, and filters that apply as they change

  document.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-href]");
    if (!row || event.target.closest("a, button, form") || !getSelection().isCollapsed) return;
    if (event.metaKey || event.ctrlKey) window.open(row.dataset.href, "_blank");
    else location.href = row.dataset.href;
  });

  document.addEventListener("change", (event) => {
    const form = event.target.closest("form[data-autosubmit]");
    if (form && event.target.tagName === "SELECT") form.requestSubmit();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "/" || event.target.closest("input, select, textarea, dialog")) return;
    const field = document.querySelector(".filters input");
    if (field) {
      event.preventDefault();
      field.focus();
    }
  });

  // Copying a payload

  document.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-copy]");
    if (!button) return;
    const source = document.querySelector(button.dataset.copy);
    try {
      await navigator.clipboard.writeText(source.textContent);
      button.textContent = "Copied";
    } catch {
      button.textContent = "Select and copy";
    }
    setTimeout(() => { button.textContent = "Copy"; }, 1600);
  });

  // Payload highlighting

  const token = /("(?:\\.|[^"\\])*")(\s*:)?|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|\b(true|false|null)\b/g;

  function highlight(root) {
    for (const pre of root.querySelectorAll("[data-json]:not([data-highlighted])")) {
      const text = pre.textContent;
      pre.dataset.highlighted = "";
      if (text.length > 200_000) continue;
      const out = document.createDocumentFragment();
      let last = 0;
      for (const match of text.matchAll(token)) {
        if (match.index > last) out.append(text.slice(last, match.index));
        const span = document.createElement("span");
        if (match[1]) {
          span.className = match[2] ? "j-key" : "j-str";
          span.textContent = match[1];
          out.append(span, match[2] || "");
        } else {
          span.className = match[3] ? "j-num" : "j-lit";
          span.textContent = match[0];
          out.append(span);
        }
        last = match.index + match[0].length;
      }
      out.append(text.slice(last));
      pre.replaceChildren(out);
    }
  }

  // Notices

  function showNotices(root) {
    for (const toast of root.querySelectorAll("[data-toast]")) {
      setTimeout(() => toast.classList.add("leaving"), 4000);
      setTimeout(() => toast.remove(), 4400);
    }
    const url = new URL(location.href);
    if (url.searchParams.has("done")) {
      url.searchParams.delete("done");
      history.replaceState(null, "", url);
    }
  }

  // Chart of the counts, sampled while the overview is open

  const SERIES = [
    { key: "available", state: "available", area: true },
    { key: "running", state: "running", area: true },
    { key: "waiting", state: "scheduled" },
    { key: "dead", state: "dead" },
  ];
  let samples = [];
  try { samples = JSON.parse(session.get(KEYS.samples)) || []; } catch { samples = []; }

  function sample(root) {
    const chart = root.querySelector("[data-chart]");
    if (!chart) return;
    const now = Date.now();
    const point = { t: now };
    for (const s of SERIES) point[s.key] = Number(chart.dataset[s.key]) || 0;
    const last = samples[samples.length - 1];
    if (!last || now - last.t >= 1000) samples.push(point);
    samples = samples.filter((p) => now - p.t <= CHART_WINDOW_MS);
    session.set(KEYS.samples, JSON.stringify(samples));
    draw(chart);
  }

  const SVG = "http://www.w3.org/2000/svg";
  const el = (name, attrs) => {
    const node = document.createElementNS(SVG, name);
    for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
    return node;
  };

  function niceMax(value) {
    if (value <= 4) return 4;
    const step = 10 ** Math.floor(Math.log10(value));
    return Math.ceil(value / step) * step;
  }

  function draw(chart) {
    chart.querySelector("svg")?.remove();
    if (samples.length < 2) return;
    chart.classList.add("has-data");
    const box = chart.getBoundingClientRect();
    const style = getComputedStyle(chart);
    const width = box.width - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
    const height = box.height - parseFloat(style.paddingTop) - parseFloat(style.paddingBottom);
    if (width <= 0 || height <= 0) return;

    const left = 44, bottom = 18, top = 6;
    const plotW = width - left, plotH = height - bottom - top;
    const t0 = samples[0].t, t1 = samples[samples.length - 1].t;
    const max = niceMax(Math.max(...samples.flatMap((p) => SERIES.map((s) => p[s.key]))));
    const x = (t) => left + ((t - t0) / Math.max(t1 - t0, 1)) * plotW;
    const y = (v) => top + plotH - (v / max) * plotH;

    const svg = el("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "Task counts over time" });
    for (const fraction of [0, 0.5, 1]) {
      const v = max * fraction;
      svg.append(el("line", { class: "grid-line", x1: left, x2: width, y1: y(v), y2: y(v) }));
      const label = el("text", { class: "axis-label", x: left - 8, y: y(v) + 4, "text-anchor": "end" });
      label.textContent = Math.round(v).toLocaleString();
      svg.append(label);
    }
    const since = el("text", { class: "axis-label", x: left, y: height - 2 });
    since.textContent = new Date(t0).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    const until = el("text", { class: "axis-label", x: width, y: height - 2, "text-anchor": "end" });
    until.textContent = "now";
    svg.append(since, until);

    for (const s of SERIES) {
      const points = samples.map((p) => `${x(p.t).toFixed(1)},${y(p[s.key]).toFixed(1)}`);
      const group = el("g", { class: `state-${s.state}` });
      if (s.area) {
        group.append(el("path", {
          class: "area",
          d: `M${x(t0)},${y(0)} L${points.join(" L")} L${x(t1)},${y(0)} Z`,
        }));
      }
      group.append(el("polyline", { class: "series", points: points.join(" ") }));
      svg.append(group);
    }
    chart.append(svg);
  }

  // Live updates: every few seconds the page fetches itself again and swaps
  // its content in place, unless the reader is busy with it.

  const toggle = document.querySelector("[data-live-toggle]");
  let live = local.get(KEYS.live) !== "off";
  let fetching = false;

  function showLive() {
    if (!toggle) return;
    toggle.setAttribute("aria-pressed", String(live));
    toggle.title = live ? "Pause live updates" : "Resume live updates";
    toggle.querySelector(".live-label").textContent = live ? "Live" : "Paused";
  }

  toggle?.addEventListener("click", () => {
    live = !live;
    local.set(KEYS.live, live ? null : "off");
    showLive();
    refresh();
  });

  function busy() {
    if (document.hidden || dialog?.open) return true;
    const active = document.activeElement;
    if (active && active.matches("input, select, textarea")) return true;
    return !getSelection().isCollapsed;
  }

  async function refresh() {
    const main = document.getElementById("main");
    if (!live || fetching || !main || !("live" in main.dataset) || busy()) return;
    fetching = true;
    try {
      const response = await fetch(location.href, { cache: "no-store", credentials: "same-origin" });
      if (!response.ok || response.redirected) return;
      const next = new DOMParser().parseFromString(await response.text(), "text/html");
      const content = next.getElementById("main");
      if (!content || busy()) return;
      main.replaceChildren(...content.childNodes);
      for (const part of next.querySelectorAll("[data-live-part]")) {
        document.querySelector(`[data-live-part="${part.dataset.livePart}"]`)?.replaceWith(part);
      }
      enhance(main);
    } catch {
      // The server is out of reach for a moment: the next tick tries again.
    } finally {
      fetching = false;
    }
  }

  function enhance(root) {
    highlight(root);
    showNotices(root);
    sample(root);
  }

  showLive();
  enhance(document);
  setInterval(refresh, REFRESH_MS);
  document.addEventListener("visibilitychange", refresh);
  if ("ResizeObserver" in window) {
    new ResizeObserver(() => {
      const chart = document.querySelector("[data-chart]");
      if (chart) draw(chart);
    }).observe(document.body);
  }
})();
