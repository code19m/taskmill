// The interface works without this script. It keeps pages live, applies
// filters as they change, confirms actions, draws a chart of the counts, and
// adds a few conveniences.
//
// It is careful with the network and the machine: one request at a time,
// each abandoned after a while, with a growing wait while the server is out
// of reach. An update changes only the parts of the page that changed, so
// nothing flickers, and focus, scroll, and open menus stay as they are.
(() => {
  "use strict";

  const REFRESH_MS = 5000; // between live updates
  const TIMEOUT_MS = 10000; // a request that takes longer is abandoned
  const MAX_WAIT_MS = 60000; // the longest wait between failed updates
  const SLOW_MS = 300; // a navigation that takes longer shows progress
  const FILTER_WAIT_MS = 250; // filter changes this close apply together
  const CHART_WINDOW_MS = 10 * 60 * 1000;
  const CHART_GAP_MS = 15000; // a longer gap between samples breaks the chart's lines
  const KEYS = { live: "taskmill-live", samples: "taskmill-samples" };

  const root = document.documentElement;
  const coarse = matchMedia("(pointer: coarse)");
  const reducedMotion = matchMedia("(prefers-reduced-motion: reduce)");

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

  // The static files of this page. A page from a newer server has others,
  // and then the whole page reloads, so that new markup never meets old code.
  const assetsOf = (doc) => [...doc.querySelectorAll("head script[src], head link[rel=stylesheet]")]
    .map((node) => node.getAttribute("src") || node.getAttribute("href")).join(" ");
  const assets = assetsOf(document);

  const progress = (on) => root.classList.toggle("loading", on);

  // Theme: System, Light, or Dark

  document.addEventListener("click", (event) => {
    const option = event.target.closest?.("[data-theme-option]");
    if (option) window.TaskmillTheme?.set(option.dataset.themeOption);
  });
  window.TaskmillTheme?.apply();

  // Actions: a confirmation first, then a pending state, so that a second
  // click on a slow network cannot send an action twice.

  const dialog = document.getElementById("confirm-dialog");
  let confirming = null;
  let submitting = false;

  function pending(form) {
    form.dataset.jsPending = "";
    submitting = true;
    setTimeout(() => {
      for (const button of form.querySelectorAll("button")) button.disabled = true;
    });
    setTimeout(() => { if (submitting) progress(true); }, SLOW_MS);
  }

  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;
    if ("filters" in form.dataset) {
      event.preventDefault();
      applyFilters(form, 0);
      return;
    }
    if ("jsPending" in form.dataset) {
      event.preventDefault();
      return;
    }
    if (form.dataset.confirm && typeof dialog?.showModal === "function") {
      event.preventDefault();
      const danger = "confirmDanger" in form.dataset;
      const button = dialog.querySelector("[data-confirm-button]");
      dialog.querySelector("h2").textContent = form.dataset.confirmTitle || "Are you sure?";
      dialog.querySelector("[data-confirm-text]").textContent = form.dataset.confirm;
      button.textContent = form.dataset.confirmLabel || "Confirm";
      button.classList.toggle("primary", !danger);
      button.classList.toggle("danger-solid", danger);
      // Escape closes the dialog without a value: an earlier one must not count.
      dialog.returnValue = "";
      confirming = form;
      dialog.showModal();
      return;
    }
    if (form.dataset.confirm && !window.confirm(`${form.dataset.confirmTitle || ""} ${form.dataset.confirm}`.trim())) {
      event.preventDefault();
      return;
    }
    if (form.method === "post") pending(form);
  });

  dialog?.addEventListener("close", () => {
    const form = confirming;
    confirming = null;
    if (dialog.returnValue !== "confirm" || !form?.isConnected) return;
    pending(form);
    form.submit(); // skips the submit event, so the dialog does not open again
  });

  // A page restored by the back button is live again.
  window.addEventListener("pageshow", (event) => {
    if (!event.persisted) return;
    submitting = false;
    progress(false);
    for (const form of document.querySelectorAll("form[data-js-pending]")) {
      delete form.dataset.jsPending;
      for (const button of form.querySelectorAll("button")) button.disabled = false;
    }
    schedule(0);
  });

  // Multiple choice filters: menus of checkboxes that apply as they change

  const openMenus = () => document.querySelectorAll("details.sel[open]");
  const boxes = (sel) => [...sel.querySelectorAll(".chk-input")];

  function label(sel) {
    const chosen = boxes(sel).filter((box) => box.checked).map((box) => box.value);
    const summary = sel.querySelector("summary");
    const text = summary.querySelector(".sel-label");
    const plural = sel.dataset.plural || "";
    text.textContent = chosen.length === 0 ? `All ${plural}` : chosen.length === 1 ? chosen[0] : `${chosen.length} ${plural}`;
    summary.classList.toggle("empty", chosen.length === 0);
    summary.setAttribute("aria-label", `${sel.querySelector(".pop").getAttribute("aria-label")}: ${text.textContent}`);
  }

  function search(sel, query) {
    const q = query.trim().toLowerCase();
    for (const option of sel.querySelectorAll(".pop-opt")) {
      option.hidden = q !== "" && !option.textContent.toLowerCase().includes(q);
    }
  }

  // place opens a menu toward the side with room.
  function place(sel) {
    const pop = sel.querySelector(".pop");
    pop.classList.remove("pop-end", "pop-up");
    const box = pop.getBoundingClientRect();
    if (box.right > innerWidth - 8) pop.classList.add("pop-end");
    if (box.bottom > innerHeight - 8 && sel.getBoundingClientRect().top > box.height + 16) pop.classList.add("pop-up");
  }

  document.addEventListener("toggle", (event) => {
    const sel = event.target;
    if (!(sel instanceof HTMLDetailsElement) || !sel.matches(".sel")) return;
    const field = sel.querySelector("[data-sel-search]");
    if (!sel.open) {
      if (field?.value) {
        field.value = "";
        search(sel, "");
      }
      return;
    }
    for (const other of openMenus()) if (other !== sel) other.open = false;
    place(sel);
    if (!coarse.matches) (field || boxes(sel)[0])?.focus({ preventScroll: true });
  }, true);

  let pointerDown = false;
  document.addEventListener("pointerdown", (event) => {
    pointerDown = true;
    for (const sel of openMenus()) if (!sel.contains(event.target)) sel.open = false;
  }, true);
  for (const type of ["pointerup", "pointercancel"]) {
    document.addEventListener(type, () => { pointerDown = false; }, true);
  }

  // Tab out of a menu closes it. A click inside it can move focus to the page
  // around it, which is not leaving.
  document.addEventListener("focusout", (event) => {
    const sel = event.target.closest?.("details.sel[open]");
    const next = event.relatedTarget;
    if (sel && next && !sel.contains(next) && !next.contains(sel)) sel.open = false;
  });

  document.addEventListener("keydown", (event) => {
    const sel = event.target.closest?.("details.sel[open]");
    if (!sel) return;
    if (event.key === "Escape") {
      event.preventDefault();
      sel.open = false;
      sel.querySelector("summary").focus();
    } else if (event.key === "Enter" && event.target.matches(".chk-input")) {
      event.preventDefault();
      event.target.click();
    } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const items = [sel.querySelector("[data-sel-search]"), ...boxes(sel).filter((box) => !box.closest(".pop-opt").hidden)]
        .filter(Boolean);
      const at = items.indexOf(document.activeElement);
      const step = event.key === "ArrowDown" ? 1 : -1;
      items[Math.min(items.length - 1, Math.max(0, at + step))]?.focus();
    }
  });

  document.addEventListener("input", (event) => {
    const target = event.target;
    if (target.matches?.("[data-sel-search]")) {
      search(target.closest(".sel"), target.value);
    } else if (target.matches?.(".filter-text") && target.form) {
      // Typed text applies with Enter; until then, Apply shows.
      const dirty = [...target.form.querySelectorAll(".filter-text")].some((input) => input.value !== input.defaultValue);
      if (dirty) target.form.dataset.jsDirty = "";
      else delete target.form.dataset.jsDirty;
    }
  });

  document.addEventListener("change", (event) => {
    const box = event.target;
    if (!box.matches?.(".sel .chk-input")) return;
    label(box.closest(".sel"));
    if (box.form && "filters" in box.form.dataset) applyFilters(box.form, FILTER_WAIT_MS);
  });

  let filterTimer = 0;
  let filterWaiting = false;

  function applyFilters(form, wait) {
    clearTimeout(filterTimer);
    filterWaiting = true;
    filterTimer = setTimeout(() => {
      filterWaiting = false;
      const url = new URL(form.action, location.href);
      for (const [name, value] of new FormData(form)) {
        if (typeof value === "string" && value !== "") url.searchParams.append(name, value);
      }
      navigate(url);
    }, wait);
  }

  // navigate shows another state of the page in place, such as other filters.
  // If that fails, the browser loads it and shows what went wrong.
  async function navigate(url) {
    const slow = setTimeout(() => progress(true), SLOW_MS);
    const result = await load(url.href, null);
    clearTimeout(slow);
    progress(false);
    if (result.kind === "aborted" || filterWaiting) return; // a newer change takes over
    if (result.kind !== "page" || assetsOf(result.doc) !== assets) {
      location.assign(url.href);
      return;
    }
    history.replaceState(history.state, "", url.href);
    apply(result);
    for (const form of document.querySelectorAll("form[data-filters]")) delete form.dataset.jsDirty;
    succeeded();
  }

  // Rows that lead somewhere, a shortcut to the filters, and copying

  document.addEventListener("click", (event) => {
    const row = event.target.closest?.("tr[data-href]");
    if (!row || event.target.closest("a, button, form, input, label, summary")) return;
    if (window.getSelection()?.isCollapsed === false) return;
    if (event.metaKey || event.ctrlKey) window.open(row.dataset.href, "_blank", "noopener");
    else location.assign(row.dataset.href);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "/" || event.metaKey || event.ctrlKey || event.altKey) return;
    if (event.target.closest?.("input, select, textarea, dialog, [contenteditable]")) return;
    const field = document.querySelector(".filters .filter-text");
    if (field) {
      event.preventDefault();
      field.focus();
      field.select();
    }
  });

  document.addEventListener("click", async (event) => {
    const button = event.target.closest?.("[data-copy]");
    const source = button && document.querySelector(button.dataset.copy);
    if (!source) return;
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

  function highlight(scope) {
    for (const pre of scope.querySelectorAll("[data-json]:not([data-js-highlighted])")) {
      const text = pre.textContent;
      pre.dataset.jsHighlighted = "";
      if (text.length > 50_000) continue; // plain text is kinder to slow machines
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

  // Notices after an action

  for (const toast of document.querySelectorAll("[data-toast]")) {
    setTimeout(() => toast.classList.add("leaving"), 4000);
    setTimeout(() => toast.remove(), 4400);
  }
  {
    const url = new URL(location.href);
    if (url.searchParams.has("done")) {
      url.searchParams.delete("done");
      history.replaceState(history.state, "", url);
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
  try {
    const saved = JSON.parse(session.get(KEYS.samples));
    if (Array.isArray(saved)) samples = saved.filter((p) => p && Number.isFinite(p.t));
  } catch { samples = []; }
  const saveSamples = () => session.set(KEYS.samples, JSON.stringify(samples));
  window.addEventListener("pagehide", saveSamples);

  function sample() {
    const chart = document.querySelector("[data-chart]");
    if (!chart) return;
    const now = Date.now();
    const point = { t: now };
    for (const s of SERIES) point[s.key] = Number(chart.dataset[s.key]) || 0;
    const last = samples[samples.length - 1];
    if (!last || now - last.t >= 1000) samples.push(point);
    samples = samples.filter((p) => now - p.t <= CHART_WINDOW_MS);
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
    if (samples.length < 2) return;
    const style = getComputedStyle(chart);
    const width = chart.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
    const height = chart.clientHeight - parseFloat(style.paddingTop) - parseFloat(style.paddingBottom);
    if (width <= 0 || height <= 0) return;

    const left = 44, bottom = 18, top = 6;
    const plotW = width - left, plotH = height - bottom - top;
    const t0 = samples[0].t, t1 = samples[samples.length - 1].t;
    const max = niceMax(Math.max(...samples.flatMap((p) => SERIES.map((s) => p[s.key]))));
    const x = (t) => (left + ((t - t0) / Math.max(t1 - t0, 1)) * plotW).toFixed(1);
    const y = (v) => (top + plotH - (v / max) * plotH).toFixed(1);

    // Samples are missing while the tab was hidden or the server out of
    // reach: the lines break there instead of bridging the gap.
    const runs = [[samples[0]]];
    for (let i = 1; i < samples.length; i++) {
      if (samples[i].t - samples[i - 1].t > CHART_GAP_MS) runs.push([]);
      runs[runs.length - 1].push(samples[i]);
    }

    const svg = el("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "Task counts over time" });
    for (const fraction of [0, 0.5, 1]) {
      const v = max * fraction;
      svg.append(el("line", { class: "grid-line", x1: left, x2: width, y1: y(v), y2: y(v) }));
      const tick = el("text", { class: "axis-label", x: left - 8, y: Number(y(v)) + 4, "text-anchor": "end" });
      tick.textContent = Math.round(v).toLocaleString();
      svg.append(tick);
    }
    const since = el("text", { class: "axis-label", x: left, y: height - 2 });
    since.textContent = new Date(t0).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    const until = el("text", { class: "axis-label", x: width, y: height - 2, "text-anchor": "end" });
    until.textContent = "now";
    svg.append(since, until);

    for (const s of SERIES) {
      const group = el("g", { class: `state-${s.state}` });
      const line = runs.map((run) => `M${run.map((p) => `${x(p.t)},${y(p[s.key])}`).join("L")}`).join("");
      if (s.area) {
        const area = runs.map((run) => `M${x(run[0].t)},${y(0)}L${run.map((p) => `${x(p.t)},${y(p[s.key])}`).join("L")}L${x(run[run.length - 1].t)},${y(0)}Z`).join("");
        group.append(el("path", { class: "area", d: area }));
      }
      group.append(el("path", { class: "series", d: line }));
      svg.append(group);
    }
    const old = chart.querySelector("svg");
    if (old) old.replaceWith(svg);
    else chart.append(svg);
    chart.classList.add("has-data");
  }

  {
    const chart = document.querySelector("[data-chart]");
    if (chart && "ResizeObserver" in window) {
      let width = chart.clientWidth;
      let frame = 0;
      new ResizeObserver(() => {
        if (chart.clientWidth === width) return;
        width = chart.clientWidth;
        cancelAnimationFrame(frame);
        frame = requestAnimationFrame(() => draw(chart));
      }).observe(chart);
    }
  }

  // Updating a page in place. morph changes the page to match a newer copy
  // of it, touching only what differs. Rows with a data-key are matched by
  // it, so a new row slides in without rebuilding the others. State that
  // belongs to the reader stays: an open menu, a checkbox or field they
  // changed, and attributes the script set, named data-js-*.

  const keyOf = (node) => (node.nodeType === 1 ? node.getAttribute("data-key") ?? (node.id || null) : null);
  const kept = (node, name) => name.startsWith("data-js-") || (name === "open" && node.localName === "details");

  function syncAttributes(node, next) {
    for (const { name } of [...node.attributes]) {
      if (!next.hasAttribute(name) && !kept(node, name)) node.removeAttribute(name);
    }
    for (const { name, value } of next.attributes) {
      if (node.getAttribute(name) !== value && !kept(node, name)) node.setAttribute(name, value);
    }
  }

  function morph(node, next) {
    if (node.hasAttribute("data-chart")) {
      // The chart's drawing is the script's; only its counts come from the server.
      for (const { name, value } of next.attributes) {
        if (name.startsWith("data-") && node.getAttribute(name) !== value) node.setAttribute(name, value);
      }
      return;
    }
    syncAttributes(node, next);
    let current = node.firstChild;
    for (const wanted of [...next.childNodes]) {
      const key = keyOf(wanted);
      let match = null;
      if (key !== null) {
        for (let n = current; n; n = n.nextSibling) {
          if (keyOf(n) === key) { match = n; break; }
        }
      } else if (current && keyOf(current) === null) {
        match = current;
      }
      if (!match || match.nodeType !== wanted.nodeType || match.nodeName !== wanted.nodeName) {
        node.insertBefore(wanted, current);
        continue;
      }
      if (match === current) current = current.nextSibling;
      else node.insertBefore(match, current);
      if (match.nodeType !== 1) {
        if (match.nodeValue !== wanted.nodeValue) match.nodeValue = wanted.nodeValue;
      } else if (match.hasAttribute("data-json")) {
        // A highlighted payload is kept while its text is the same.
        if (match.textContent !== wanted.textContent) match.replaceWith(wanted);
      } else {
        morph(match, wanted);
      }
    }
    while (current) {
      const next = current.nextSibling;
      current.remove();
      current = next;
    }
  }

  // load fetches a page. It is the only request in flight: a newer one
  // abandons it. The result's kind says what happened: "page", "same" (not
  // modified), "down" (out of reach, or a server error), "signed-out", or
  // "aborted".
  let inflight = null;

  async function load(url, etag) {
    inflight?.abort();
    const control = new AbortController();
    inflight = control;
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; control.abort(); }, TIMEOUT_MS);
    try {
      const headers = { "X-Taskmill-Live": "1" };
      if (etag) headers["If-None-Match"] = etag;
      const response = await fetch(url, {
        headers, cache: "no-store", credentials: "same-origin", redirect: "manual", signal: control.signal,
      });
      if (response.status === 304) return { kind: "same" };
      // A redirect leads to a login page, outside of the interface.
      if (response.type === "opaqueredirect" || response.status === 401 || response.status === 403) {
        return { kind: "signed-out" };
      }
      if (response.status >= 500 || response.status === 429) return { kind: "down" };
      if (!(response.headers.get("Content-Type") || "").startsWith("text/html")) return { kind: "down" };
      const doc = new DOMParser().parseFromString(await response.text(), "text/html");
      const main = doc.getElementById("main");
      if (!main) return { kind: "down" };
      return { kind: "page", doc, main, etag: response.headers.get("ETag") };
    } catch {
      return { kind: control.signal.aborted && !timedOut ? "aborted" : "down" };
    } finally {
      clearTimeout(timer);
      if (inflight === control) inflight = null;
    }
  }

  let shownETag = null; // of the page on screen, once an update fetched it

  function apply(result) {
    const main = document.getElementById("main");
    morph(main, result.main);
    for (const part of result.doc.querySelectorAll("[data-live-part]")) {
      const here = document.querySelector(`[data-live-part="${CSS.escape(part.dataset.livePart)}"]`);
      if (here) morph(here, part);
    }
    if (result.doc.title) document.title = result.doc.title;
    shownETag = result.etag;
    highlight(main);
  }

  // Live updates: every few seconds the page fetches itself and updates in
  // place, unless the reader is busy with it. The button in the header shows
  // whether the page is live, paused, out of date, or signed out.

  const toggle = document.querySelector("[data-live-toggle]");
  const LABELS = { live: "Live", paused: "Paused", stale: "Reconnecting", offline: "Offline", "signed-out": "Signed out" };
  let live = local.get(KEYS.live) !== "off";
  let status = "live";
  let failures = 0;
  let lastUpdate = Date.now();
  let timer = 0;

  const livePage = () => document.getElementById("main")?.hasAttribute("data-live") ?? false;

  function show() {
    if (!toggle) return;
    let state = live ? status : "paused";
    if (state === "stale" && navigator.onLine === false) state = "offline";
    toggle.dataset.state = state;
    toggle.setAttribute("aria-pressed", String(live));
    toggle.querySelector(".live-label").textContent = LABELS[state];
    const since = new Date(lastUpdate).toLocaleTimeString();
    toggle.title = {
      live: "Live updates are on. Click to pause them.",
      paused: "Live updates are paused. Click to resume them.",
      stale: `The server did not answer. Trying again; last update at ${since}.`,
      offline: `This device is offline. Last update at ${since}.`,
      "signed-out": "The session ended. Click to reload the page.",
    }[state];
  }

  function busy() {
    if (submitting || pointerDown || dialog?.open || document.querySelector("details.sel[open]")) return true;
    if (document.activeElement?.matches("input:not([type=checkbox]), select, textarea")) return true;
    return window.getSelection()?.isCollapsed === false;
  }

  function schedule(ms) {
    clearTimeout(timer);
    timer = setTimeout(update, ms);
  }

  function succeeded(took = 0) {
    failures = 0;
    lastUpdate = Date.now();
    status = "live";
    show();
    sample();
    if (toggle && !reducedMotion.matches && toggle.animate) {
      toggle.querySelector(".live-dot")?.animate([{ opacity: 0.3 }, { opacity: 1 }], { duration: 700, easing: "ease-out" });
    }
    // A slow server gets more time between updates.
    schedule(Math.max(REFRESH_MS, 2 * took));
  }

  async function update() {
    if (!live || status === "signed-out" || document.hidden || !livePage()) return;
    if (busy() || filterWaiting) {
      schedule(1000);
      return;
    }
    const started = Date.now();
    const result = await load(location.href, shownETag);
    if (result.kind === "aborted" || !live) return; // a navigation took over, or the reader paused
    if (result.kind === "page") {
      if (assetsOf(result.doc) !== assets) {
        location.reload();
        return;
      }
      if (busy() || filterWaiting) {
        schedule(1000);
        return;
      }
      apply(result);
    }
    if (result.kind === "page" || result.kind === "same") {
      succeeded(Date.now() - started);
      return;
    }
    status = result.kind === "signed-out" ? "signed-out" : "stale";
    show();
    if (status === "stale") {
      failures++;
      schedule(Math.min(MAX_WAIT_MS, REFRESH_MS * 2 ** failures));
    }
  }

  toggle?.addEventListener("click", () => {
    if (status === "signed-out") {
      location.reload();
      return;
    }
    live = !live;
    local.set(KEYS.live, live ? null : "off");
    show();
    if (live) {
      failures = 0;
      schedule(0);
    } else {
      clearTimeout(timer);
    }
  });

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      saveSamples();
      return;
    }
    schedule(Math.max(0, REFRESH_MS - (Date.now() - lastUpdate)));
  });
  window.addEventListener("online", () => {
    failures = 0;
    schedule(0);
  });
  window.addEventListener("offline", show);

  highlight(document);
  sample();
  show();
  if (live) schedule(REFRESH_MS);
})();
