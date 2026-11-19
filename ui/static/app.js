// The interface works without this script. It keeps pages live, shows
// another page, a filter, or the result of an action without loading the
// page anew, confirms actions, and adds a few conveniences.
//
// It is careful with the network and the machine: one page request at a
// time, each abandoned after a while, with a growing wait while the server
// is out of reach. An update changes only the parts of the page that
// changed, so nothing flickers, and focus, scroll, and open menus stay as
// they are. Whatever it cannot show in place, the browser loads as a page.
(() => {
  "use strict";

  const REFRESH_MS = 5000; // between live updates
  const TIMEOUT_MS = 10000; // a request that takes longer is abandoned
  const MAX_WAIT_MS = 60000; // the longest wait between failed updates
  const SLOW_MS = 300; // a navigation that takes longer shows progress
  const SEARCH_WAIT_MS = 250; // typing this close asks the server once
  const NOTICE_MS = 4000; // a notice stays this long
  const FADE_MS = 140; // another page fades in
  const KEEP_PAGES = 10; // pages kept in memory for the back and forward buttons
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
  // replaces fades back.
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

  // send runs a form's action and shows the page that the server answers
  // with, in place: the page that the action leads to, with its notice, or
  // the page that says why the action did not apply.
  async function send(form) {
    const buttons = [...form.querySelectorAll("button")];
    const turn = ++turns;
    form.dataset.jsPending = "";
    submitting = true;
    for (const button of buttons) button.disabled = true;
    const slow = setTimeout(() => progress(true), SLOW_MS);
    const result = await request(form.action, { body: new URLSearchParams(new FormData(form)) });
    clearTimeout(slow);
    submitting = false;
    progress(false);
    delete form.dataset.jsPending;
    for (const button of buttons) button.disabled = false;
    if (result.kind === "signed-out") {
      status = "signed-out";
      show();
    }
    if (result.kind !== "page") {
      // Nothing says whether the action reached the server.
      notify(document.querySelector("[data-failure]").content.firstElementChild.cloneNode(true), 2 * NOTICE_MS);
      return;
    }
    if (turn !== turns) return; // the reader went on to another page meanwhile
    // The server led to the page that shows the result, or it answered with
    // a page that says why the action did not apply.
    const url = result.url ? clean(new URL(result.url)) : new URL(location.href);
    if (assetsOf(result.doc) !== assets) {
      location.assign(url.href);
      progress(true);
      return;
    }
    const elsewhere = url.pathname !== location.pathname;
    const n = address(url, elsewhere ? "push" : "replace");
    if (n == null) return;
    render(result, { moved: elsewhere || !result.url, n, notice: result.doc.querySelector("[data-toast]") });
    succeeded();
  }

  // settle ends the pending state of every form. A page that was kept while
  // an action was on its way comes back with it.
  function settle() {
    for (const form of document.querySelectorAll("form[data-js-pending]")) {
      delete form.dataset.jsPending;
      for (const button of form.querySelectorAll("button")) button.disabled = false;
    }
  }

  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;
    if ("filters" in form.dataset) {
      event.preventDefault();
      applyFilters(form);
      return;
    }
    if (form.method !== "post") return;
    event.preventDefault();
    if ("jsPending" in form.dataset) return;
    if (form.dataset.confirm && typeof dialog?.showModal === "function") {
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
    if (form.dataset.confirm && !window.confirm(`${form.dataset.confirmTitle || ""} ${form.dataset.confirm}`.trim())) return;
    send(form);
  });

  dialog?.addEventListener("close", () => {
    const form = confirming;
    confirming = null;
    if (dialog.returnValue === "confirm" && form?.isConnected) send(form);
  });

  // A page restored by the back button is live again.
  window.addEventListener("pageshow", (event) => {
    if (!event.persisted) return;
    submitting = false;
    progress(false);
    settle();
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
    if (form && "filters" in form.dataset) applyFilters(form);
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
    }, SEARCH_WAIT_MS);
  }

  // refine shows what a menu's search finds: among the options that the
  // menu lists, and on the server among those that it does not.
  function refine(sel) {
    const field = sel.querySelector("[data-sel-search]");
    if (!field) return;
    search(sel, field.value);
    findMore(sel, field.value);
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

  // A live update waits while the reader scrolls: rows changing under a
  // scroll make it stutter on a slow machine.
  let scrolledAt = 0;
  document.addEventListener("scroll", () => { scrolledAt = Date.now(); }, { capture: true, passive: true });

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
    if (field.matches?.("[data-sel-search]")) refine(field.closest(".sel"));
  });

  document.addEventListener("change", (event) => {
    const box = event.target;
    if (!box.matches?.(".sel .chk-input")) return;
    label(box.closest(".sel"));
    if (box.form && "filters" in box.form.dataset) applyFilters(box.form);
  });

  // applyFilters shows the page for the filters at once. A change made while
  // the last one loads abandons it, so quick clicks cost no more than one.
  function applyFilters(form) {
    const url = new URL(form.action, location.href);
    for (const [name, value] of new FormData(form)) {
      if (typeof value === "string" && value !== "") url.searchParams.append(name, value);
    }
    navigate(url, "replace");
  }

  // The history. Each entry gets a number, and the page that it showed is
  // kept under that number while the tab lives, with its scroll position.
  // So the back and forward buttons show a page right away, where the
  // reader left it.
  const pages = new Map();
  const stamp = () => Date.now() + Math.random();
  const here = () => location.pathname + location.search;
  let entry = history.state?.n; // of the page on screen
  let shown = here(); // the address of the page on screen
  if (entry == null) {
    entry = stamp();
    history.replaceState({ n: entry }, "");
  }

  // leave keeps the page on screen, before another one takes its place.
  function leave() {
    pages.delete(entry);
    pages.set(entry, { html: root.outerHTML, y: scrollY });
    if (pages.size > KEEP_PAGES) pages.delete(pages.keys().next().value);
  }

  // address gives the page that is about to show its place in the history,
  // and returns the number of its entry. "push" adds an entry, as a link
  // does, and "replace" changes the address of this one, as a filter does.
  // A frame without an origin of its own may not change its address: then
  // the browser loads the page, and address returns nothing.
  function address(url, mode) {
    try {
      if (mode === "replace" || url.href === location.href) {
        history.replaceState({ n: entry }, "", url.href);
        return entry;
      }
      leave();
      const n = stamp();
      history.pushState({ n }, "", url.href);
      return n;
    } catch {
      location.assign(url.href);
      progress(true);
      return null;
    }
  }

  // navigate shows another page, or another state of this one, in place,
  // with its place in the history as address gives it. Whatever cannot be
  // shown in place, the browser loads, and shows what went wrong.
  let turns = 0; // counts navigations and actions: a newer one takes over

  async function navigate(url, mode) {
    const turn = ++turns;
    const slow = setTimeout(() => progress(true), SLOW_MS);
    const result = await load(url.href, null);
    clearTimeout(slow);
    if (turn !== turns || result.kind === "aborted") return; // a newer change takes over
    if (result.kind !== "page" || assetsOf(result.doc) !== assets) {
      location.assign(url.href);
      progress(true);
      return;
    }
    progress(false);
    const n = address(url, mode);
    if (n == null) return;
    render(result, { moved: mode === "push", n });
    succeeded();
  }

  // render puts the page of history entry n on screen. A newer state of the
  // page that is there changes in place, as a live update does. Another
  // page replaces it: it fades in, starts at the top, or at y for the back
  // button, and tells a screen reader its title. The fade only follows the
  // change, so a browser that paints late, or not at all, still has the
  // page.
  function render(result, { moved, n, y = 0, notice = null }) {
    inflight?.abort(); // an update of the page that this one replaces
    const focused = document.activeElement;
    apply(result, moved);
    entry = n;
    shown = here();
    if (notice) notify(notice);
    const main = document.getElementById("main");
    const announcer = document.querySelector("[data-announce]");
    if (!moved) {
      // The change may remove the element that had focus, as the last "Load
      // more" removes its link: the page takes the focus, not the body. And
      // a list that is longer, or has other filters, tells a screen reader
      // how many tasks it shows now. An action says what it did itself.
      if (focused?.isConnected === false) main.focus({ preventScroll: true });
      if (!notice && announcer) announcer.textContent = main.querySelector("[data-shown]")?.textContent ?? "";
      return;
    }
    scrollTo(0, y);
    main.focus({ preventScroll: true });
    if (!reducedMotion.matches) main.animate?.([{ opacity: 0.4 }, { opacity: 1 }], { duration: FADE_MS, easing: "ease-out" });
    if (announcer) announcer.textContent = document.title;
  }

  // The back and forward buttons. A page that is kept shows right away, as
  // it was, and then updates. One that is not, the browser loads.
  window.addEventListener("popstate", (event) => {
    let n = event.state?.n;
    // Another place on the page that is on screen, as the link to its
    // content leads to: the browser scrolls there. Its entry is new to the
    // script, and gets the page's number.
    if (n == null && here() === shown) {
      n = entry;
      history.replaceState({ n }, "");
    }
    if (n === entry) return;
    const was = pages.get(n);
    const result = was && page(was.html);
    if (!result) {
      location.reload();
      return;
    }
    turns++; // a page or an action that is on its way was for the page before
    progress(false);
    leave();
    render(result, { moved: true, n, y: was.y });
    settle();
    schedule(0);
  });

  // Links, rows that lead somewhere, a shortcut to the filters, and copying

  // destination returns the page that a click asks for in this tab, or
  // null: the address of a link, or of a row that leads somewhere. The
  // browser follows the other links itself: to another site or tab, a
  // download, or a place on this page.
  function destination(event) {
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return null;
    const link = event.target.closest?.("a[href]");
    if (link) {
      if ((link.target && link.target !== "_self") || link.hasAttribute("download")) return null;
      const url = new URL(link.href);
      if (url.origin !== location.origin) return null;
      if (url.hash && url.pathname === location.pathname && url.search === location.search) return null;
      return url;
    }
    const row = event.target.closest?.("tr[data-href]");
    if (!row || event.target.closest("button, form, input, label, summary")) return null;
    return new URL(row.dataset.href, location.href);
  }

  // A click on a link, or on a row. Last, so that a handler above can take
  // the click instead.
  document.addEventListener("click", (event) => {
    if (event.defaultPrevented) return;
    const hit = event.target.closest?.("a, tr[data-href]");
    if (hit?.localName === "tr" && (event.metaKey || event.ctrlKey) && !event.target.closest("button, form, input, label, summary")) {
      window.open(hit.dataset.href, "_blank", "noopener");
      return;
    }
    const url = destination(event);
    // A click that ends a selection of text in a row is no click on the row.
    if (!url || (hit?.localName === "tr" && window.getSelection()?.isCollapsed === false)) return;
    event.preventDefault();
    // More of a list is another state of its page, as a filter is: it
    // changes in place, and the reader stays where they are. The place in
    // its link is for a browser that loads the longer list as a page.
    const more = hit?.matches("a[data-more]");
    if (more) url.hash = "";
    navigate(url, more ? "replace" : "push");
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

  // Notices after an action. The server names a notice in the address of
  // the page that shows it. The address loses that right away, so a reload
  // does not show the notice again.

  function clean(url) {
    for (const name of ["done", "count", "skipped", "conflicts"]) url.searchParams.delete(name);
    return url;
  }

  // notify shows a notice in place of the one before it, for a while. The
  // time counts once the tab is in view: a notice must not come and go while
  // the reader is elsewhere.
  function notify(toast, ms = NOTICE_MS) {
    for (const old of document.querySelectorAll(".toast")) if (old !== toast) old.remove();
    if (!document.contains(toast)) document.querySelector(".main").before(toast); // from the page that was fetched
    const start = () => {
      setTimeout(() => toast.classList.add("leaving"), ms);
      setTimeout(() => toast.remove(), ms + 400);
    };
    if (document.hidden) document.addEventListener("visibilitychange", start, { once: true });
    else start();
  }

  {
    const toast = document.querySelector("[data-toast]");
    if (toast) notify(toast);
    const url = clean(new URL(location.href));
    if (url.href !== location.href) history.replaceState({ n: entry }, "", url);
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

  // page reads a page of the interface from its HTML. It returns nothing
  // for other HTML, such as a login page.
  function page(html) {
    const doc = new DOMParser().parseFromString(html, "text/html");
    const main = doc.getElementById("main");
    return main ? { kind: "page", doc, main } : null;
  }

  // request fetches a page, or with a body, sends an action and reads the
  // page that the server answers with. The result's kind says what happened:
  // "page", "same" (not modified), "down" (out of reach, or a server error),
  // "signed-out", or "aborted". The page of an action may be one that says
  // why the action did not apply. Its url is where the server led to, if it
  // led somewhere.
  async function request(url, { etag = null, body = null, control = new AbortController() } = {}) {
    let timedOut = false;
    // An action may work for as long as the server lets a request take.
    const timer = setTimeout(() => { timedOut = true; control.abort(); }, body ? 3 * TIMEOUT_MS : TIMEOUT_MS);
    try {
      const headers = { "X-Taskmill-Live": "1" };
      if (etag) headers["If-None-Match"] = etag;
      const response = await fetch(url, {
        method: body ? "POST" : "GET", body, headers, cache: "no-store", credentials: "same-origin",
        // A page that redirects leads to a login page, outside of the
        // interface. An action redirects to the page that shows its result.
        redirect: body ? "follow" : "manual", signal: control.signal,
      });
      if (response.status === 304) return { kind: "same" };
      if (response.type === "opaqueredirect" || response.status === 401 || (response.status === 403 && !body)) {
        return { kind: "signed-out" };
      }
      if (!body && (response.status >= 500 || response.status === 429)) return { kind: "down" };
      if (!(response.headers.get("Content-Type") || "").startsWith("text/html")) return { kind: "down" };
      const result = page(await response.text());
      if (!result) return { kind: body && response.redirected ? "signed-out" : "down" };
      return { ...result, etag: response.headers.get("ETag"), url: response.redirected ? response.url : null };
    } catch {
      return { kind: control.signal.aborted && !timedOut ? "aborted" : "down" };
    } finally {
      clearTimeout(timer);
    }
  }

  // load fetches a page. It is the only page request in flight: a newer one
  // abandons it.
  let inflight = null;

  async function load(url, etag) {
    inflight?.abort();
    const control = new AbortController();
    inflight = control;
    try {
      return await request(url, { etag, control });
    } finally {
      if (inflight === control) inflight = null;
    }
  }

  let shownETag = null; // of the page on screen, once an update fetched it

  // apply changes the page on screen into the one of result. A newer state
  // of the same page changes only where it differs. Another page, fresh,
  // replaces it whole: nothing of the reader's state belongs to it.
  function apply(result, fresh = false) {
    const main = document.getElementById("main");
    if (fresh) {
      syncAttributes(main, result.main);
      main.replaceChildren(...result.main.childNodes);
    } else {
      morph(main, result.main);
    }
    for (const part of result.doc.querySelectorAll("[data-live-part]")) {
      const here = document.querySelector(`[data-live-part="${CSS.escape(part.dataset.livePart)}"]`);
      if (here) morph(here, part);
    }
    if (result.doc.title) document.title = result.doc.title;
    shownETag = result.etag;
    highlight(main);
    // The newer copy lists every option of a menu, and none that a search
    // found on the server: an open menu shows its search again.
    for (const sel of openMenus()) refine(sel);
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
    if (Date.now() - scrolledAt < 1000) return true;
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
    // A slow server gets more time between updates.
    schedule(Math.max(REFRESH_MS, 2 * took));
  }

  async function update() {
    timer = 0;
    if (!live || status === "signed-out" || document.hidden || !livePage()) return;
    // A request in flight is a navigation: it must not be abandoned for an
    // update of the page it replaces.
    if (busy() || inflight) {
      schedule(1000);
      return;
    }
    const started = Date.now();
    const result = await load(location.href, shownETag);
    if (!live) return; // the reader paused
    if (result.kind === "aborted") { // another page took over: it plans the next update, or this does
      if (!timer) schedule(REFRESH_MS);
      return;
    }
    if (result.kind === "page") {
      if (assetsOf(result.doc) !== assets) {
        location.reload();
        return;
      }
      if (busy()) {
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
