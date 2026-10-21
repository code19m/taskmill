// The interface works without this script. It keeps pages live, applies
// filters as they change, confirms actions, and adds a few conveniences.
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
  const LIVE_KEY = "taskmill-live";

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

  // The static files of this page. A page from a newer server has others,
  // and then the whole page reloads, so that new markup never meets old code.
  const assetsOf = (doc) => [...doc.querySelectorAll("head script[src], head link[rel=stylesheet]")]
    .map((node) => node.getAttribute("src") || node.getAttribute("href")).join(" ");
  const assets = assetsOf(document);

  // While the next page loads, a bar slides under the header and the page it
  // replaces fades back. Leaving shows it for a full load, which the browser
  // marks only in its own toolbar, if at all.
  const progress = (on) => root.classList.toggle("loading", on);
  const leaving = () => setTimeout(() => progress(true), SLOW_MS);

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
      button.replaceChildren(form.dataset.confirmLabel || "Confirm");
      const icon = form.querySelector(".icon");
      if (icon) button.prepend(icon.cloneNode(true));
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

  // visible shows control characters, such as the separator in the outbox's
  // group keys, as the server does.
  const visible = (text) => text.replace(/[\u0000-\u001f]/g, (c) => String.fromCharCode(0x2400 + c.charCodeAt(0)));

  // search shows the options that contain the query, and offers to filter
  // by the query itself when no option is exactly it.
  function search(sel, query) {
    const q = query.trim().toLowerCase();
    for (const row of sel.querySelectorAll(".pop-list .pop-opt")) {
      row.hidden = q !== "" && !row.textContent.toLowerCase().includes(q);
    }
    const add = sel.querySelector("[data-sel-add]");
    if (!add) return;
    const value = query.trim();
    add.hidden = value === "" || boxes(sel).some((box) => box.value === value);
    add.dataset.value = value;
    add.textContent = `Filter by “${visible(value)}”`;
  }

  // option builds a menu option like the server's, for a value that the menu
  // does not list yet.
  function option(sel, value, checked) {
    const tick = document.querySelector(".chk svg");
    const row = document.createElement("label");
    row.className = "pop-opt";
    row.dataset.key = value;
    const box = document.createElement("input");
    box.type = "checkbox";
    box.className = "chk-input";
    box.name = sel.dataset.name;
    box.value = value;
    box.checked = checked;
    const mark = document.createElement("span");
    mark.className = "chk";
    if (tick) mark.append(tick.cloneNode(true));
    const text = document.createElement("span");
    text.className = "mono" in sel.dataset ? "pop-text mono" : "pop-text";
    text.textContent = visible(value);
    row.append(box, mark, text);
    sel.querySelector(".pop-empty")?.remove();
    return row;
  }

  // addValue chooses a value that the menu does not list, typed in its search.
  function addValue(sel, value) {
    sel.querySelector(".pop-list").prepend(option(sel, value, true));
    const field = sel.querySelector("[data-sel-search]");
    field.value = "";
    search(sel, "");
    field.focus();
    label(sel);
    const form = sel.closest("form");
    if (form && "filters" in form.dataset) applyFilters(form, 0);
  }

  // A menu with more choices than it lists asks the server for those that
  // match its search, and adds them.
  let lookup = null;
  let lookupTimer = 0;

  function findMore(sel, query) {
    clearTimeout(lookupTimer);
    lookup?.abort();
    if (!sel.dataset.source || query.trim() === "") return;
    lookupTimer = setTimeout(async () => {
      const control = new AbortController();
      lookup = control;
      const timer = setTimeout(() => control.abort(), TIMEOUT_MS);
      try {
        const url = `${sel.dataset.source}?search=${encodeURIComponent(query.trim())}`;
        const response = await fetch(url, {
          credentials: "same-origin", signal: control.signal, headers: { "X-Taskmill-Live": "1" },
        });
        if (!response.ok) return;
        const found = await response.json();
        if (!Array.isArray(found) || !sel.open) return;
        const listed = new Set(boxes(sel).map((box) => box.value));
        const list = sel.querySelector(".pop-list");
        for (const value of found) {
          if (typeof value === "string" && !listed.has(value)) list.append(option(sel, value, false));
        }
        search(sel, sel.querySelector("[data-sel-search]")?.value ?? "");
      } catch {
        // Out of reach for a moment: the menu keeps what it lists.
      } finally {
        clearTimeout(timer);
      }
    }, FILTER_WAIT_MS);
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
    } else if (event.key === "Enter" && event.target.matches("[data-sel-search]")) {
      // Enter adds the typed value, or chooses the one option that matches.
      event.preventDefault();
      const add = sel.querySelector("[data-sel-add]");
      const shown = boxes(sel).filter((box) => !box.closest(".pop-opt").hidden);
      if (add && !add.hidden) addValue(sel, add.dataset.value);
      else if (shown.length === 1) shown[0].click();
    } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const items = [sel.querySelector("[data-sel-search]"), ...boxes(sel).filter((box) => !box.closest(".pop-opt").hidden)]
        .filter(Boolean);
      const at = items.indexOf(document.activeElement);
      const step = event.key === "ArrowDown" ? 1 : -1;
      items[Math.min(items.length - 1, Math.max(0, at + step))]?.focus();
    }
  });

  document.addEventListener("click", (event) => {
    const add = event.target.closest?.("[data-sel-add]");
    if (add?.dataset.value) addValue(add.closest(".sel"), add.dataset.value);
  });

  document.addEventListener("input", (event) => {
    const field = event.target;
    if (!field.matches?.("[data-sel-search]")) return;
    const sel = field.closest(".sel");
    search(sel, field.value);
    findMore(sel, field.value);
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
    if (result.kind === "aborted" || filterWaiting) return; // a newer change takes over
    if (result.kind !== "page" || assetsOf(result.doc) !== assets) {
      location.assign(url.href);
      progress(true);
      return;
    }
    progress(false);
    history.replaceState(history.state, "", url.href);
    apply(result);
    succeeded();
  }

  // Rows that lead somewhere, a shortcut to the filters, and copying

  document.addEventListener("click", (event) => {
    const row = event.target.closest?.("tr[data-href]");
    if (!row || event.target.closest("a, button, form, input, label, summary")) return;
    if (window.getSelection()?.isCollapsed === false) return;
    if (event.metaKey || event.ctrlKey) {
      window.open(row.dataset.href, "_blank", "noopener");
    } else {
      location.assign(row.dataset.href);
      leaving();
    }
  });

  // A link to another page of this tab. Last, so that a handler above can
  // take the click instead.
  document.addEventListener("click", (event) => {
    const link = event.target.closest?.("a[href]");
    if (!link || event.defaultPrevented || event.button !== 0) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    if ((link.target && link.target !== "_self") || link.hasAttribute("download")) return;
    const url = new URL(link.href);
    if (url.origin !== location.origin) return;
    if (url.hash && url.pathname === location.pathname && url.search === location.search) return;
    leaving();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "/" || event.metaKey || event.ctrlKey || event.altKey) return;
    if (event.target.closest?.("input, select, textarea, dialog, [contenteditable]")) return;
    // "/" opens the kind filter, ready to search.
    const sel = document.querySelector('.filters details.sel[data-name="kind"]');
    if (sel) {
      event.preventDefault();
      sel.open = true;
      sel.querySelector("[data-sel-search]")?.focus();
    }
  });

  document.addEventListener("click", async (event) => {
    const button = event.target.closest?.("[data-copy]");
    const source = button && document.querySelector(button.dataset.copy);
    if (!source) return;
    const label = button.lastElementChild;
    const icon = button.querySelector("use");
    try {
      await navigator.clipboard.writeText(source.textContent);
      label.textContent = "Copied";
      icon.setAttribute("href", "#i-check");
    } catch {
      label.textContent = "Select and copy";
    }
    setTimeout(() => {
      label.textContent = "Copy";
      icon.setAttribute("href", "#i-copy");
    }, 1600);
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
    // A checkbox that now stands for another value must not keep the
    // reader's choice of the old one.
    const reused = node.localName === "input" &&
      (node.getAttribute("value") !== next.getAttribute("value") || node.getAttribute("name") !== next.getAttribute("name"));
    syncAttributes(node, next);
    if (reused) {
      node.checked = node.defaultChecked;
      node.value = node.defaultValue;
    }
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
  let live = local.get(LIVE_KEY) !== "off";
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
    if (toggle && !reducedMotion.matches && toggle.animate) {
      toggle.querySelector(".live-dot")?.animate([{ opacity: 0.3 }, { opacity: 1 }], { duration: 700, easing: "ease-out" });
    }
    // A slow server gets more time between updates.
    schedule(Math.max(REFRESH_MS, 2 * took));
  }

  async function update() {
    if (!live || status === "signed-out" || document.hidden || !livePage()) return;
    // A request in flight is a navigation: it must not be abandoned for an
    // update of the page it replaces.
    if (busy() || filterWaiting || inflight) {
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
    local.set(LIVE_KEY, live ? null : "off");
    show();
    if (live) {
      failures = 0;
      schedule(0);
    } else {
      clearTimeout(timer);
    }
  });

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) schedule(Math.max(0, REFRESH_MS - (Date.now() - lastUpdate)));
  });
  window.addEventListener("online", () => {
    failures = 0;
    schedule(0);
  });
  window.addEventListener("offline", show);

  highlight(document);
  show();
  if (live) schedule(REFRESH_MS);
})();
