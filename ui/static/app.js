// The interface works without this script. It shows another page, a filter,
// or the result of an action in place, keeps pages live, confirms actions,
// shows times in the reader's time zone and keeps them counting, and adds a
// few conveniences. It is careful with the network and the machine: one page
// request at a time, each abandoned after a while. While the server fails or
// is out of reach, the page says so, and tries again less often after each
// failure. A page that is not the dashboard's, such as a login page, the
// browser loads.
//
// Its parts, in order: Setup, Fetch, Morph and apply, Navigation, Live, Actions
// and notices, Menus, Calendar, Times, Hints and tooltips, Charts,
// Conveniences, and Start.
(() => {
  "use strict";

  // ==== Setup ====

  const REFRESH_MS = 5000; // between live updates
  const TIMEOUT_MS = 15000; // then a request is abandoned: after the server's 10 seconds, whose page says so
  const MAX_WAIT_MS = 60000; // the longest wait between failed updates: 5, 10, 20, 40, then 60 seconds
  const SLOW_MS = 300; // a navigation that takes longer shows progress
  const SEARCH_WAIT_MS = 250; // typing this close asks the server once
  const NOTICE_MS = 4000; // a notice stays this long
  const FADE_MS = 140; // another page fades in
  const KEEP_PAGES = 10; // pages kept in memory for the back and forward buttons
  const LIVE_KEY = "taskmill-live";

  const root = document.documentElement;
  const coarse = matchMedia("(pointer: coarse)");
  const reducedMotion = matchMedia("(prefers-reduced-motion: reduce)");

  // local remembers a setting on this device, where the browser allows it.
  const local = {
    get(key) {
      try { return localStorage.getItem(key); } catch { return null; }
    },
    set(key, value) {
      try {
        if (value == null) localStorage.removeItem(key);
        else localStorage.setItem(key, value);
      } catch { /* storage is blocked: nothing is remembered */ }
    },
  };

  // The static files of this page. A page from a newer server has others,
  // and then the whole page reloads, so that new markup never meets old code.
  const assetsOf = (doc) => [...doc.querySelectorAll("head script[src], head link[rel=stylesheet]")]
    .map((node) => node.getAttribute("src") || node.getAttribute("href")).join(" ");
  const assets = assetsOf(document);

  // While the next page loads, a bar slides under the header and the page it
  // replaces fades back.
  const progress = (shown) => root.classList.toggle("loading", shown);

  // on listens to an event anywhere on the page.
  const on = (type, listener, options) => document.addEventListener(type, listener, options);

  // loadWhole has the browser load a page, as it does without the script.
  function loadWhole(url) {
    location.assign(url);
    progress(true);
  }

  // ==== Fetch ====

  let inflight = null; // see load

  // page reads a page of the interface from its HTML. It returns nothing
  // for other HTML, such as a login page.
  function page(html) {
    const doc = new DOMParser().parseFromString(html, "text/html");
    const main = doc.getElementById("main");
    return main ? { kind: "page", doc, main } : null;
  }

  // timeOut abandons the request of control after TIMEOUT_MS, and returns
  // what stops the wait. The abort's reason says that time ran out.
  function timeOut(control) {
    const timeout = setTimeout(() => control.abort("timeout"), TIMEOUT_MS);
    return () => clearTimeout(timeout);
  }

  // request fetches a page, or with a body, sends an action and reads the
  // page that answers it, which may say why it did not apply. The result's
  // kind says what happened: "page", "same" (not modified), "down" (no
  // answer from the dashboard: the network, a timeout, or a proxy's error
  // page in its place), "elsewhere" (a page that is not the dashboard's),
  // "signed-out", or "aborted". A page is "failed" when it says that the
  // server failed: it tries again. Its url is where the server led, if it did.
  async function request(url, { etag = null, body = null, control = new AbortController() } = {}) {
    const stopWaiting = timeOut(control);
    try {
      const headers = { "X-Taskmill-Live": "1" };
      if (etag) headers["If-None-Match"] = etag;
      const response = await fetch(url, {
        method: body ? "POST" : "GET", body, headers, cache: "no-store", credentials: "same-origin",
        // An action is led to the page that shows its result. A page is at
        // its address: a redirect leads to a login page, or elsewhere, and
        // the browser follows it.
        redirect: body ? "follow" : "manual", signal: control.signal,
      });
      const began = performance.now();
      if (response.status === 304) return { kind: "same" };
      if (response.type === "opaqueredirect" || response.status === 401 || (response.status === 403 && !body)) {
        return { kind: "signed-out" };
      }
      const html = (response.headers.get("Content-Type") || "").startsWith("text/html");
      const result = html ? page(await response.text()) : null;
      if (!result) {
        if (response.status >= 500 || response.status === 429) return { kind: "down" };
        return { kind: body && (response.redirected || response.status === 403) ? "signed-out" : "elsewhere" };
      }
      syncServerClock(result.main, began);
      return { ...result, failed: response.status >= 500, etag: response.headers.get("ETag"),
        url: response.redirected ? response.url : null };
    } catch {
      return { kind: control.signal.aborted && control.signal.reason !== "timeout" ? "aborted" : "down" };
    } finally {
      stopWaiting();
    }
  }

  // load fetches a page. It is the only page request in flight: a newer one
  // abandons it.
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

  // ==== Morph and apply ====

  // morph changes the page to match a newer copy of it, touching only what
  // differs. State that belongs to the reader stays: an open menu, a checkbox
  // or field they changed, and what the script set: attributes named
  // data-js-*, the words of a chart's slider, and the place of a hint, in its
  // style. No copy brings a style attribute: the content security policy
  // would refuse it, so the server sends none, and a page that is kept leaves
  // it behind.
  //
  // A child with a data-key or an id is matched by it, wherever it stands.
  // What stands before the match gives way: a child that the copy lacks, and
  // text between keyed children, are removed. A child that the copy has
  // further on is in the way. If the match is the element right after it,
  // that child is the one out of place: it is set aside, and moves when the
  // copy comes to it. Otherwise the match moves before it. So a row that
  // comes, goes, or moves, up or down, is the one row that is touched, and it
  // keeps the focus. The keys are read once, when a child is first out of
  // place: the work grows with the number of children, not its square.

  let shownETag = null; // of the page on screen, once an update fetched it

  const keyOf = (node) => (node.nodeType === 1 ? node.getAttribute("data-key") ?? (node.id || null) : null);
  const kept = (node, name) => name.startsWith("data-js-") || name === "style" || name === "aria-valuetext" ||
    (name === "open" && node.localName === "details");

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

    const children = [...next.childNodes];
    let current = node.firstChild;
    let ahead = null; // the keyed children that the copy has too, and has not come to yet, by key
    let aside = null; // those of them that are set aside, behind current
    for (const [at, wanted] of children.entries()) {
      const key = keyOf(wanted);
      const inPlace = current && keyOf(current) === key ? current : null;
      if (!inPlace && key !== null && !ahead) {
        ahead = keyedFrom(current, children.slice(at));
        aside = new Set();
      }
      const match = ahead?.get(key) ?? inPlace;
      if (match && match !== current && !aside.has(match)) current = giveWay(current, match, ahead, aside);
      ahead?.delete(key);
      if (!match || match.nodeType !== wanted.nodeType || match.nodeName !== wanted.nodeName) {
        if (aside?.has(match)) match.remove(); // of another kind now, and behind current, where nothing else removes it
        node.insertBefore(wanted, current);
        continue;
      }
      if (match === current) {
        current = current.nextSibling;
      } else {
        const focused = match.contains(document.activeElement) && document.activeElement;
        node.insertBefore(match, current);
        if (focused) focused.focus({ preventScroll: true });
      }
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
      const after = current.nextSibling;
      current.remove();
      current = after;
    }
  }

  // keyedFrom maps each key of wanted to the first child from current on with it.
  function keyedFrom(current, wanted) {
    const keys = new Set(wanted.map(keyOf));
    const found = new Map();
    for (let n = current; n; n = n.nextSibling) {
      const k = keyOf(n);
      if (k !== null && keys.has(k) && !found.has(k)) found.set(k, n);
    }
    return found;
  }

  // giveWay clears the way to match, as above, and returns the child it then stands before.
  function giveWay(current, match, ahead, aside) {
    while (current !== match) {
      const before = current;
      const stays = ahead.get(keyOf(before)) === before;
      if (stays && match.previousElementSibling !== before) break;
      current = before.nextSibling;
      if (stays) aside.add(before);
      else before.remove();
    }
    return current;
  }

  // apply changes the page on screen into the one of result. A newer state
  // of the same page changes only where it differs. Another page, fresh,
  // replaces it whole: nothing of the reader's state belongs to it.
  function apply(result, fresh = false) {
    // The page changes with its titles on it. Then the tooltip shows again.
    const about = tipFor;
    const focused = document.activeElement;
    undescribe();
    localise(result.main);
    count(result.main);
    const main = document.getElementById("main");
    if (fresh) {
      syncAttributes(main, result.main);
      main.replaceChildren(...result.main.childNodes);
    } else {
      morph(main, result.main);
    }

    for (const part of result.doc.querySelectorAll("[data-live-part]")) {
      const mine = document.querySelector(`[data-live-part="${CSS.escape(part.dataset.livePart)}"]`);
      if (mine) morph(mine, part);
    }

    // The change may remove what had the focus, as the last "Load more"
    // removes its link, or an update a row: the page takes it, not the body.
    if (focused?.isConnected === false) main.focus({ preventScroll: true });
    if (result.doc.title) document.title = result.doc.title;
    shownETag = result.etag;
    highlight(main);
    for (const slider of main.querySelectorAll(".chart-step")) say(slider);
    if (traced) trace(); // the update took the guide off its chart, or the chart off the page
    if (hint?.matches(":popover-open")) placeHint();

    // The newer copy lists every option of a menu, and none that a search
    // found on the server: an open menu shows its search again.
    for (const sel of openMenus()) refine(sel);
    if (about?.isConnected) describe(about);
  }

  // ==== Navigation ====

  // Each entry of the history gets a number, and the page that it showed is
  // kept under that number while the tab lives, with its scroll position.
  // So the back and forward buttons show a page right away, where the
  // reader left it.
  const pages = new Map();
  const stamp = () => Date.now() + Math.random();
  const here = () => location.pathname + location.search;
  let entry = history.state?.n; // of the page on screen
  let onScreen = here(); // the address of the page on screen
  let turns = 0; // counts navigations and actions: a newer one takes over
  if (entry == null) {
    entry = stamp();
    history.replaceState({ n: entry }, "");
  }

  // nextTurn starts a navigation or an action, and returns its number.
  const nextTurn = () => ++turns;

  // leave keeps the page on screen, before another one takes its place:
  // its title, its content, and the parts around it that an update changes.
  // What the script put there stays out: a chart's guide, and a hint's
  // place, in its style attribute. A page that does not update itself, such
  // as one at the address of the page whose action it refused, is not kept:
  // the browser loads the address again.
  function leave() {
    untrace();
    undescribe(); // the page is kept with its titles
    pages.delete(entry);
    if (!livePage()) return;
    for (const pop of document.querySelectorAll(".hint-pop[style]")) pop.removeAttribute("style");
    const parts = [document.getElementById("main"), ...document.querySelectorAll("[data-live-part]")];
    pages.set(entry, { title: document.title, html: parts.map((part) => part.outerHTML).join(""), y: scrollY });
    if (pages.size > KEEP_PAGES) pages.delete(pages.keys().next().value);
  }

  // address gives the page that is about to show its place in the history,
  // and returns the number of its entry: "push" adds an entry, as a link
  // does, and "replace" changes this one, as a filter does. A frame without
  // an origin of its own may not change its address: the browser loads the
  // page then, and address returns nothing.
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
      loadWhole(url.href);
      return null;
    }
  }

  // navigate shows another page, or another state of this one, in place, with
  // its place in the history as address gives it. A page that says the server
  // failed, or that no answer came, shows in place too, and tries again by
  // itself; one that is not the dashboard's, or of a newer version, the
  // browser loads. In the mode "ask", the dialog shows the page, which asks
  // before an action on many tasks.
  async function navigate(url, mode) {
    const turn = nextTurn();
    const slow = setTimeout(() => progress(true), SLOW_MS);
    let result = await load(url.href, null);
    clearTimeout(slow);
    if (turn !== turns || result.kind === "aborted") return; // a newer change takes over
    progress(false);
    if (result.kind === "down") result = unreachable();
    if (result.kind !== "page" || assetsOf(result.doc) !== assets) {
      loadWhole(url.href);
      return;
    }

    if (mode === "ask" && showDialogPart(result)) return;
    const n = address(url, mode);
    if (n == null) return;
    render(result, { moved: mode !== "replace", n });
    startUpdates(result);
  }

  // unreachable returns the page that says that no answer came, from the
  // template of the page on screen. It tries its address again.
  function unreachable() {
    const doc = document.implementation.createHTMLDocument();
    doc.head.append(...[...document.head.querySelectorAll("script[src], link[rel=stylesheet]")].map((node) => node.cloneNode()));
    const main = doc.body.appendChild(document.getElementById("main").cloneNode());
    main.dataset.live = "retry";

    // No section is the current one, as on the server's status pages.
    const nav = doc.body.appendChild(document.querySelector("[data-live-part=nav]").cloneNode(true));
    for (const item of nav.querySelectorAll("[aria-current]")) item.removeAttribute("aria-current");
    main.append(document.querySelector("[data-unreachable]").content.cloneNode(true));
    doc.title = `${main.querySelector("h1").textContent} ${document.title.slice(document.title.lastIndexOf("·"))}`;
    return { kind: "page", doc, main, failed: true };
  }

  // render puts the page of history entry n on screen. A newer state of the
  // page on screen changes in place, as a live update does. Another page
  // replaces it: it fades in, starts at the top, or at y for the back
  // button, and tells a screen reader its title. The fade follows the
  // change, so a browser that paints late, or not at all, has the page.
  function render(result, { moved, n, y = 0, notice = null }) {
    inflight?.abort(); // an update of the page that this one replaces
    apply(result, moved);
    entry = n;
    onScreen = here();
    showLiveStatus(); // the page may be one that does not update itself
    if (notice) notify(notice);
    const main = document.getElementById("main");
    const announcer = document.querySelector("[data-announce]");
    // A screen reader hears it in a live region that was there before the
    // words: a notice that comes with its words may go unread.
    if (!moved) {
      // A list that is longer, or has other filters, says how many tasks it
      // shows now, or that it shows none; an action says what it did.
      if (announcer) announcer.textContent = notice?.textContent ?? main.querySelector("[data-shown], .empty-title")?.textContent ?? "";
      return;
    }

    scrollTo(0, y);
    main.focus({ preventScroll: true });
    if (!reducedMotion.matches) main.animate?.([{ opacity: 0.4 }, { opacity: 1 }], { duration: FADE_MS, easing: "ease-out" });
    if (announcer) announcer.textContent = notice ? `${document.title}. ${notice.textContent}` : document.title;
  }

  // The back and forward buttons. A page that is kept shows right away, as
  // it was, and then updates. One that is not, the browser loads.
  window.addEventListener("popstate", (event) => {
    let n = event.state?.n;
    // A place on the page on screen, as the skip link leads to: the browser
    // scrolls there, and the entry, new to the script, gets the page's number.
    if (n == null && here() === onScreen) {
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

    nextTurn(); // a page or an action that is on its way was for the page before
    progress(false);
    dialog?.close?.(); // it asked about the page before
    leave();
    result.doc.title = was.title;
    render(result, { moved: true, n, y: was.y });
    release();
    schedule(0);
  });

  // A click on a control in a row is not a click on the row.
  const CONTROLS = "button, form, input, label, summary";

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
    if (!row || event.target.closest(CONTROLS)) return null;
    return new URL(row.dataset.href, location.href);
  }

  // followLink shows the page of a click on a link, or on a row, in place,
  // unless a handler took the click.
  function followLink(event) {
    if (event.defaultPrevented) return;
    const hit = event.target.closest?.("a, tr[data-href]");
    if (hit?.localName === "tr" && (event.metaKey || event.ctrlKey) && !event.target.closest(CONTROLS)) {
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

    // A link in a menu is a filter too, a preset of the range of time: the
    // one choice closes the menu.
    const sel = hit?.closest("details.sel");
    if (sel) shut(sel);
    const asks = hit?.matches("a[data-dialog]") && typeof dialog?.showModal === "function";
    navigate(url, more || sel ? "replace" : asks ? "ask" : "push");
  }

  // ==== Live ====

  // Every few seconds the page fetches itself and updates in place, unless
  // the reader is busy with it. The Live button says whether the page is
  // live, paused, out of date, or signed out; its dot pulses while updates
  // run. While the server fails, the page on screen stays. A page that says
  // the server failed is live too: it says when it tries again, and gives
  // way to the page once the server answers.

  const toggle = document.querySelector("[data-live-toggle]");
  const LABELS = { live: "Live", paused: "Paused", stale: "Reconnecting", offline: "Offline", "signed-out": "Signed out" };
  let live = local.get(LIVE_KEY) !== "off";
  let liveStatus = "live";
  let failures = 0; // in a row
  let cause = ""; // what the last failure said
  let lastUpdate = Date.now();
  let timer = 0; // of the next update
  let due = 0; // when the next update runs
  let waiting = false; // the last update waited for the reader
  let pointerDown = false;
  let scrolledAt = 0;

  const livePage = () => document.getElementById("main")?.hasAttribute("data-live") ?? false;
  const retrying = () => document.getElementById("main")?.dataset.live === "retry";

  function showLiveStatus() {
    if (!toggle) return;
    let state = live ? liveStatus : "paused";
    if (state === "stale" && navigator.onLine === false) state = "offline";
    toggle.dataset.state = state;
    toggle.classList.toggle("running", state === "live" && !waiting && livePage());
    toggle.setAttribute("aria-pressed", String(live));
    toggle.querySelector(".live-label").textContent = LABELS[state];
    const since = new Date(lastUpdate).toLocaleTimeString();
    toggle.title = {
      live: "Live updates are on. Click to pause them.",
      paused: "Live updates are paused. Click to resume them.",
      stale: `${cause || "The dashboard did not answer"}. Trying again; last update at ${since}.`,
      offline: `This device is offline. Last update at ${since}.`,
      "signed-out": "The session ended. Click to reload the page.",
    }[state];
    if (described === toggle) describe(tipFor); // its tooltip says the new title
    showRetry();
  }

  // signOut says that the session ended.
  function signOut() {
    liveStatus = "signed-out";
    showLiveStatus();
  }

  // showRetry says, on a page that says the server failed, when it tries again.
  function showRetry() {
    const said = document.querySelector("[data-retry]");
    if (!said) return;
    const s = Math.ceil((due - Date.now()) / 1000);
    said.textContent = !live ? "Live updates are paused" : liveStatus === "signed-out" ? ""
      : s > 0 ? `Trying again in ${s}s` : "Trying again…";
  }

  on("pointerdown", () => { pointerDown = true; }, true);
  // A press ends when the pointer lifts. A context menu, on macOS and Linux,
  // and a window that comes to the front take the lift with them: the live
  // updates must not wait for it.
  const lift = () => { pointerDown = false; };
  for (const type of ["pointerup", "pointercancel", "contextmenu"]) on(type, lift, true);
  window.addEventListener("blur", lift);

  // A live update waits while the reader scrolls: rows changing under a
  // scroll make it stutter on a slow machine.
  on("scroll", () => { scrolledAt = Date.now(); }, { capture: true, passive: true });

  // The reader is busy during a press, an action, or a scroll, while a
  // dialog or a menu is open or text is selected, and while a field has the
  // focus: also the slider of a chart, whose steps must not change under it.
  function busy() {
    if (submitting || pointerDown || dialog?.open || document.querySelector("details.sel[open]")) return true;
    if (Date.now() - scrolledAt < 1000) return true;
    if (document.activeElement?.matches("input:not([type=checkbox]), select, textarea")) return true;
    return window.getSelection()?.isCollapsed === false;
  }

  function schedule(ms) {
    clearTimeout(timer);
    due = Date.now() + ms;
    timer = setTimeout(update, ms);
  }

  function succeeded(took = 0) {
    failures = 0;
    lastUpdate = Date.now();
    liveStatus = "live";
    // A slow server gets more time between updates.
    schedule(Math.max(REFRESH_MS, 2 * took));
    showLiveStatus();
  }

  // heading is a page's title: on a status page, what failed.
  const heading = (main) => main.querySelector("h1")?.textContent ?? "";

  // failed counts a failure, says what it was, and waits longer before the
  // next update: 5 seconds after the first, and twice as long after each
  // more, up to a minute.
  function failed(why = "") {
    failures++;
    cause = why;
    liveStatus = "stale";
    schedule(Math.min(MAX_WAIT_MS, REFRESH_MS * 2 ** (failures - 1)));
    showLiveStatus();
  }

  // startUpdates starts afresh the updates of a page that the reader asked
  // for: a page that says the server failed tries again 5 seconds later.
  function startUpdates(result) {
    failures = 0;
    if (result.failed) failed(heading(document.getElementById("main")));
    else succeeded();
  }

  async function update() {
    timer = 0;
    if (!live || liveStatus === "signed-out" || document.hidden || !livePage()) return;
    const wasWaiting = waiting;
    waiting = busy();
    if (waiting !== wasWaiting) showLiveStatus();

    // A request in flight is a navigation: it must not be abandoned for an
    // update of the page it replaces.
    if (waiting || inflight) {
      schedule(1000);
      return;
    }

    const started = Date.now();
    showRetry();
    const result = await load(location.href, shownETag);
    if (!live) return; // the reader paused
    if (result.kind === "aborted") { // another page took over: it plans the next update, or this does
      if (!timer) schedule(REFRESH_MS);
      return;
    }
    if (result.kind === "signed-out") {
      signOut();
      return;
    }
    if (result.kind !== "page" && result.kind !== "same") {
      failed();
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
      // A page that says the server failed keeps the page on screen, unless
      // that says so too. The page gives way to the first that works.
      if (result.failed) {
        const why = heading(result.main);
        if (retrying()) apply(result);
        failed(why);
        return;
      }
      if (retrying()) render(result, { moved: true, n: entry });
      else apply(result);
    }

    succeeded(Date.now() - started);
  }

  toggle?.addEventListener("click", () => {
    if (liveStatus === "signed-out") {
      location.reload();
      return;
    }

    live = !live;
    local.set(LIVE_KEY, live ? null : "off");
    showLiveStatus();
    if (live) {
      failures = 0;
      schedule(0);
    } else {
      clearTimeout(timer);
    }
  });

  // A tab that comes back in view updates when its update is due: at once
  // if it fell due while the tab was hidden, which runs none.
  on("visibilitychange", () => {
    if (document.hidden) return;
    schedule(Math.max(0, (failures ? due : lastUpdate + REFRESH_MS) - Date.now()));
    tick();
  });
  window.addEventListener("online", () => {
    failures = 0;
    schedule(0);
  });
  window.addEventListener("offline", showLiveStatus);

  // ==== Actions and notices ====

  // An action asks first, in the dialog, and is then pending, so that a
  // second click on a slow network cannot send it twice.
  const dialog = document.getElementById("confirm-dialog");
  let confirming = null; // the form that the dialog asks about
  let submitting = false;

  // The notice of an action that got no answer says what may have happened:
  // an offline device sent nothing, and an ended session let nothing
  // through; otherwise the action may have applied before the answer was lost.
  const UNSENT = "This device is offline, so the action was not sent.";
  const REFUSED = "The session ended, so the action did not apply. Reload the page to sign in again.";
  const UNCONFIRMED = "No answer came for this action. It may have applied: check the page before you try again.";

  function failureToast(text) {
    const toast = document.querySelector("[data-failure]").content.firstElementChild.cloneNode(true);
    toast.querySelector(".toast-message").textContent = text;
    return toast;
  }

  // unanswered says why an action got no page in answer, and has the page
  // show what is true now, as soon as the dashboard answers.
  function unanswered(result, offline) {
    if (result.kind === "signed-out") {
      signOut();
      return REFUSED;
    }
    schedule(0);
    return offline ? UNSENT : UNCONFIRMED;
  }

  // openDialog shows the dialog: its title, text, and status line, and its
  // main button, which reads as content and sends form. Without a form, the
  // dialog only says something, and closes.
  function openDialog(title, text, form, content = [], look = "", said = "") {
    const [cancel, button] = dialog.querySelectorAll("button");
    dialog.querySelector("h2").textContent = title;
    dialog.querySelector("[data-confirm-status]").textContent = said;
    dialog.querySelector("[data-confirm-text]").replaceChildren(...text);
    cancel.textContent = form ? "Cancel" : "Close";
    button.hidden = !form;
    button.className = look;
    button.replaceChildren(...content);

    // Escape closes the dialog without a value: an earlier one must not count.
    dialog.returnValue = "";
    confirming = form;
    if (!dialog.open) dialog.showModal();
    if (!form) cancel.focus(); // the main button is gone, and may have had the focus
  }

  // showDialogPart shows in the dialog the part of a page that names its
  // title, if the page has one: what an action on many tasks would change,
  // its next step, or why it failed. The part's filters and sentences are
  // the text, its form's button the main button, and the page's notice, of
  // the last step, the status.
  function showDialogPart(result) {
    const part = result.main.querySelector("[data-dialog-title]");
    if (!part) return false;
    const form = part.querySelector("form");
    const button = form?.querySelector("button");
    openDialog(part.dataset.dialogTitle, part.querySelectorAll("dl, p"), form, button?.childNodes, button?.className,
      result.doc.querySelector("[data-toast]")?.textContent);
    return true;
  }

  // showActionResult shows the page that answered an action, in place: the
  // page that the action leads to, with its notice, or the page that says
  // why the action did not apply.
  function showActionResult(result) {
    const url = result.url ? clean(new URL(result.url)) : new URL(location.href);
    if (assetsOf(result.doc) !== assets) {
      loadWhole(url.href);
      return;
    }

    const elsewhere = url.pathname !== location.pathname;
    const n = address(url, elsewhere ? "push" : "replace");
    if (n == null) return;
    render(result, { moved: elsewhere || !result.url, n, notice: result.doc.querySelector("[data-toast]") });
    startUpdates(result);
  }

  // setPending marks a form whose action is on its way, and disables its
  // buttons, or ends that.
  function setPending(form, pending) {
    form.toggleAttribute("data-js-pending", pending);
    for (const button of form.querySelectorAll("button")) button.disabled = pending;
  }

  // send runs the action of a form on the page, and shows the page that the
  // server answers with.
  async function send(form) {
    const turn = nextTurn();
    setPending(form, true);
    submitting = true;

    const slow = setTimeout(() => progress(true), SLOW_MS);
    const offline = navigator.onLine === false;
    const result = await request(form.action, { body: new URLSearchParams(new FormData(form)) });

    clearTimeout(slow);
    submitting = false;
    progress(false);
    setPending(form, false);
    if (result.kind !== "page") {
      notify(failureToast(unanswered(result, offline)), 2 * NOTICE_MS);
      return;
    }

    if (turn !== turns) return; // the reader went on to another page meanwhile
    showActionResult(result);
  }

  // act runs the action that the dialog asks about for a page, and keeps the
  // dialog for the answer: the next step, or why the action failed; for the
  // page of the result, it closes. Its status line says that the action
  // runs, or that no answer came. A reader who closed it gets the answer.
  async function act() {
    if (submitting) return; // a second press sends nothing
    const said = dialog.querySelector("[data-confirm-status]");
    const button = dialog.querySelector("[data-confirm-button]");
    const turn = nextTurn();
    submitting = true;
    // The button is unavailable meanwhile, and keeps the focus. The dialog is
    // not marked busy: a screen reader would not read the status line then.
    button.ariaDisabled = "true";
    said.textContent = said.dataset.busy;
    const offline = navigator.onLine === false;
    const result = await request(confirming.action, { body: new URLSearchParams(new FormData(confirming)) });
    submitting = false;
    button.ariaDisabled = null;
    if (result.kind !== "page") {
      const text = unanswered(result, offline);
      if (dialog.open) said.textContent = text;
      else notify(failureToast(text), 2 * NOTICE_MS);
      return;
    }

    if (turn !== turns) return;
    if (assetsOf(result.doc) === assets && showDialogPart(result)) return;
    dialog.close();
    showActionResult(result);
  }

  // release ends the pending state of every form. A page that was kept while
  // an action was on its way comes back with it.
  function release() {
    for (const form of document.querySelectorAll("form[data-js-pending]")) setPending(form, false);
  }

  on("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;
    if ("filters" in form.dataset) {
      event.preventDefault();
      // With the script, the form's button is the one of the filter by
      // time. A range that is refused keeps its menu open.
      if (!applyFilters(form)) return;
      const sel = event.submitter?.closest("details.sel");
      if (sel) shut(sel);
      return;
    }

    // The dialog's own form closes it. For a form that came with a page,
    // the dialog stays, and shows the answer.
    if (form.method === "dialog" && event.submitter?.value === "confirm" && confirming && confirming.ownerDocument !== document) {
      event.preventDefault();
      act();
      return;
    }
    if (form.method !== "post") return;
    event.preventDefault();
    if ("jsPending" in form.dataset) return;
    if (form.dataset.confirm && typeof dialog?.showModal === "function") {
      const icon = form.querySelector(".icon");
      openDialog(form.dataset.confirmTitle || "Are you sure?", [form.dataset.confirm], form,
        [...(icon ? [icon.cloneNode(true)] : []), form.dataset.confirmLabel || "Confirm"],
        "confirmDanger" in form.dataset ? "btn danger-solid" : "btn primary");
      return;
    }

    if (form.dataset.confirm && !window.confirm(`${form.dataset.confirmTitle || ""} ${form.dataset.confirm}`.trim())) return;
    send(form);
  });

  // The close event comes a moment after the dialog closed. If the dialog
  // opened again meanwhile, the event is of the closing before.
  dialog?.addEventListener("close", () => {
    if (dialog.open) return;
    const form = confirming;
    confirming = null;
    if (dialog.returnValue === "confirm" && form?.isConnected) send(form);
  });

  // A page restored by the back button is live again.
  window.addEventListener("pageshow", (event) => {
    if (!event.persisted) return;
    submitting = false;
    progress(false);
    release();
    schedule(0);
  });

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
    if (document.hidden) on("visibilitychange", start, { once: true });
    else start();
  }

  {
    const toast = document.querySelector("[data-toast]");
    if (toast) notify(toast);
    const url = clean(new URL(location.href));
    if (url.href !== location.href) history.replaceState({ n: entry }, "", url);
  }

  // ==== Menus ====

  // Multiple choice filters: menus of checkboxes that apply as they change.
  let lookup = null; // a search on the server
  let lookupTimer = 0;

  const openMenus = () => document.querySelectorAll("details.sel[open]");
  const boxes = (sel) => [...sel.querySelectorAll(".chk-input")];

  function label(sel) {
    const chosen = boxes(sel).filter((box) => box.checked);
    const summary = sel.querySelector("summary");
    const text = summary.querySelector(".sel-label");
    const plural = sel.dataset.plural || "";

    // The one choice shows as its option does.
    if (chosen.length === 1) text.replaceChildren(...chosen[0].parentNode.querySelector(".pop-text").cloneNode(true).childNodes);
    else text.textContent = chosen.length ? `${chosen.length} ${plural}` : `All ${plural}`;
    summary.classList.toggle("empty", chosen.length === 0);
    summary.setAttribute("aria-label", `${sel.querySelector(".pop").getAttribute("aria-label")}: ${text.textContent}`);
  }

  // visible shows the characters of a name that no one could see as the
  // server does: a control character as its picture, if it has one, and any
  // other, or a format character, as its code point.
  const visible = (text) => text.replace(/[\p{Cc}\p{Cf}]/gu, (c) => (c < " " ? String.fromCharCode(0x2400 + c.charCodeAt(0))
    : c === "\x7f" ? "\u2421" : `<U+${c.codePointAt(0).toString(16).toUpperCase().padStart(4, "0")}>`));

  // group shows a FIFO group key as the server's "group" does: a key that
  // the outbox made as its topic and its message key, and any other as it is.
  function group(key) {
    const parts = /^([^\u001f]+)\u001f([^\u001f]+)$/.exec(key);
    if (!parts || visible(parts[1] + parts[2]) !== parts[1] + parts[2]) return visible(key);
    const newSpan = (name, text) => Object.assign(document.createElement("span"), { className: name, textContent: text });
    const shown = document.createElement("span");
    const sep = newSpan("topic-sep", "›");
    sep.ariaHidden = "true";
    shown.title = `Outbox topic ${parts[1]}, key ${parts[2]}`;
    shown.append(newSpan("topic", parts[1]), sep, parts[2]);
    return shown;
  }

  // shut closes a menu, and gives the focus back to its button.
  function shut(sel) {
    sel.open = false;
    sel.querySelector("summary").focus();
  }

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
    const checkmark = document.querySelector(".chk svg");
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
    if (checkmark) mark.append(checkmark.cloneNode(true));
    const text = document.createElement("span");
    text.className = "mono" in sel.dataset ? "pop-text mono" : "pop-text";
    text.append(sel.dataset.name === "group" ? group(value) : visible(value));
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

  // findMore asks the server for the choices that match a search, for a
  // menu with more choices than it lists, and adds them.
  function findMore(sel, query) {
    clearTimeout(lookupTimer);
    lookup?.abort();
    if (!sel.dataset.source || query.trim() === "") return;
    lookupTimer = setTimeout(async () => {
      const control = new AbortController();
      lookup = control;
      const stopWaiting = timeOut(control);
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
        stopWaiting();
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

  // placeMenu opens a menu toward the side with room.
  function placeMenu(sel) {
    const pop = sel.querySelector(".pop");
    pop.classList.remove("pop-end", "pop-up");
    const box = pop.getBoundingClientRect();
    if (box.right > innerWidth - 8) pop.classList.add("pop-end");
    if (box.bottom > innerHeight - 8 && sel.getBoundingClientRect().top > box.height + 16) pop.classList.add("pop-up");
  }

  on("toggle", (event) => {
    const sel = event.target;
    if (!(sel instanceof HTMLDetailsElement) || !sel.matches(".sel")) return;
    const field = sel.querySelector("[data-sel-search]");
    if (!sel.open) {
      if (field?.value) {
        field.value = "";
        search(sel, "");
      }
      // A closed menu forgets a range that was picked or typed and not applied.
      for (const input of sel.querySelectorAll("[data-bound]")) {
        input.value = input.defaultValue;
        input.setCustomValidity("");
      }
      return;
    }

    for (const other of openMenus()) if (other !== sel) other.open = false;
    if (sel.matches(".sel-range")) openCalendar(sel);
    placeMenu(sel);
    if (!coarse.matches) (field || boxes(sel)[0])?.focus({ preventScroll: true });
  }, true);

  // A press outside a menu closes it.
  on("pointerdown", (event) => {
    for (const sel of openMenus()) if (!sel.contains(event.target)) sel.open = false;
  }, true);

  // Tab out of a menu closes it. A click inside it can move focus to the page
  // around it, which is not leaving.
  on("focusout", (event) => {
    const sel = event.target.closest?.("details.sel[open]");
    const next = event.relatedTarget;
    if (sel && next && !sel.contains(next) && !next.contains(sel)) sel.open = false;
  });

  on("keydown", (event) => {
    const sel = event.target.closest?.("details.sel[open]");
    if (!sel) return;
    if (event.key === "Escape") {
      event.preventDefault();
      shut(sel);
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
    } else if ((event.key === "ArrowDown" || event.key === "ArrowUp") && !event.target.closest(".range-custom")) {
      // A custom range has arrow keys of its own: in its calendar, and in its fields.
      event.preventDefault();
      const items = [
        sel.querySelector("[data-sel-search]"), ...boxes(sel).filter((box) => !box.closest(".pop-opt").hidden),
        ...sel.querySelectorAll("a.pop-opt"),
      ].filter(Boolean);
      const at = items.indexOf(document.activeElement);
      const step = event.key === "ArrowDown" ? 1 : -1;
      items[Math.min(items.length - 1, Math.max(0, at + step))]?.focus();
    }
  });

  on("click", (event) => {
    const add = event.target.closest?.("[data-sel-add]");
    if (add?.dataset.value) addValue(add.closest(".sel"), add.dataset.value);
  });

  on("input", (event) => {
    if (event.target.matches?.("[data-sel-search]")) refine(event.target.closest(".sel"));
  });

  on("change", (event) => {
    const box = event.target;
    if (!box.matches?.(".sel .chk-input")) return;
    label(box.closest(".sel"));
    if (box.form && "filters" in box.form.dataset) applyFilters(box.form);
  });

  // applyFilters shows the page for the filters at once. A change made while
  // the last one loads abandons it, so quick clicks cost no more than one.
  // A range of time that is not one stays: its field says so, not a page
  // of its own, and applyFilters returns false.
  function applyFilters(form) {
    const url = new URL(form.action, location.href);
    for (const [name, value] of new FormData(form)) {
      if (typeof value === "string" && value !== "") url.searchParams.append(name, value);
    }

    // The address takes the bounds as instants, not what the browser's fields hold.
    const range = form.querySelector(".sel-range");
    if (range) {
      const asks = ["since", "before"].map((name) => asked(range, name));
      if (asks[0] && asks[1] && asks[0] >= asks[1]) {
        boundOf(range, "before")[0].setCustomValidity("The range must not end before it starts.");
      }
      const refused = range.querySelector("[data-bound]:invalid");
      if (refused) {
        refused.reportValidity();
        return false;
      }
      url.searchParams.delete("since");
      url.searchParams.delete("before");
      if (asks[0]) url.searchParams.set("since", asks[0]);
      if (asks[1]) url.searchParams.set("before", asks[1]);
    }

    navigate(url, "replace");
    return true;
  }

  // ==== Calendar ====

  // The filter by time: a calendar, and two bounds to type. To is the last
  // minute of the range: the address takes the start of the next one, as
  // "before". So a range of days ends at 23:59, with all of its last day. A
  // day is its date as text, "2026-11-19". The calendar counts days in UTC:
  // only a bound is read on the reader's clock.

  const DAY = 864e5;
  const iso = (ms) => new Date(ms).toISOString().slice(0, 10);
  const today = () => dateOf(new Date());
  const boundOf = (sel, name) => sel.querySelectorAll(`[data-bound=${name}]`); // its date, and its time
  const dayName = new Intl.DateTimeFormat("en-GB", { dateStyle: "full", timeZone: "UTC" });
  const monthName = new Intl.DateTimeFormat("en-GB", { month: "long", year: "numeric", timeZone: "UTC" });
  let weekStart = 1; // Monday, unless the browser's language says otherwise
  try {
    const locale = new Intl.Locale(navigator.language);
    weekStart = (locale.getWeekInfo?.() ?? locale.weekInfo).firstDay % 7;
  } catch { /* the browser does not say */ }
  // The open menu's calendar: "at" is the day with the tab stop, whose
  // month shows, "first" started a range, and "over" would end it.
  let cal = null;

  // openCalendar shows a menu's calendar on the last day of its range, or on today.
  function openCalendar(sel) {
    cal = {};
    move(dayOf(boundOf(sel, "before")[0].value) ?? today());
  }

  // dayOf reads a date as a field holds it, with or without its dashes.
  function dayOf(text) {
    const m = /^(\d{4})-?(\d\d)-?(\d\d)$/.exec(text.trim());
    const day = m && m.slice(1).join("-");
    return day && iso(Date.UTC(m[1], m[2] - 1, m[3])) === day ? day : null;
  }

  // asked returns the instant that a bound of the range asks for, "" for no
  // bound, or null if it is no date and time, which its field then says. A
  // bound that the reader left as it was asks for the server's instant:
  // where the clocks go back, one time on the clock is two instants. A date
  // alone is a whole day, and a time alone is of today.
  function asked(sel, name) {
    const [date, time] = boundOf(sel, name);
    const last = name === "before";
    if (date.value === date.defaultValue && time.value === time.defaultValue) {
      return sel.querySelector(`[name=${name}]`).dataset.time;
    }

    if (!date.value && !time.value) return "";
    const day = dayOf(date.value || today());
    const m = /^(\d\d?):?(\d\d)(?::?(\d\d))?$/.exec(time.value.trim() || (last ? "23:59" : "00:00"));
    if (!day) {
      date.setCustomValidity("Write a date as 2026-11-19.");
    } else if (!m || m[1] > 23 || m[2] > 59 || m[3] > 59) {
      time.setCustomValidity("Write a time as 14:05.");
    } else {
      date.value = day;
      time.value = `${pad(m[1])}:${m[2]}${m[3] ? `:${m[3]}` : ""}`;
      // Where the clocks go back, From is the first time that the clock
      // shows it, and To the last. The range ends after To's minute, or
      // second.
      const [y, month, d] = day.split("-");
      let at = new Date(y, month - 1, d, m[1], m[2], m[3] ?? 0);
      if (last) {
        const again = new Date(+at + 36e5);
        at = new Date(+(again.getHours() === at.getHours() ? again : at) + (m[3] ? 1000 : 60000));
      }
      return `${at.toISOString().slice(0, 19)}Z`;
    }
    return null;
  }

  // months returns the day n months from a day, or the last of that month.
  function months(day, n) {
    const [y, m, d] = day.split("-").map(Number);
    return iso(Date.UTC(y, m - 1 + n, Math.min(d, new Date(Date.UTC(y, m + n, 0)).getUTCDate())));
  }

  // draw shows the calendar in the open menu or, before an update, in the
  // newer copy of it: the update then finds it there, and touches nothing.
  function draw(scope) {
    const sel = document.querySelector(".sel-range[open]");
    if (!sel) { // the menu closed, or left with its page
      cal = null;
      return;
    }

    scope ??= sel;
    const grid = scope.querySelector(".cal-grid");
    if (!grid.firstChild) {
      grid.innerHTML = `<div class="cal-row" role="row">${'<button type="button" role="gridcell"></button>'.repeat(7)}</div>`.repeat(6);
    }

    const now = today();
    const month = cal.at.slice(0, 7);
    const [from, to] = ["since", "before"].map((name) => dayOf(boundOf(sel, name)[0].value));

    // The range of the fields, or the one that a click would make.
    const [lo, hi] = cal.first && cal.over ? [cal.first, cal.over].sort() : [from, to];
    const first = Date.parse(`${month}-01`);
    const start = first - ((new Date(first).getUTCDay() - weekStart + 7) % 7) * DAY;

    // The title is said when it changes: only then is it written.
    const title = scope.querySelector(".cal-title");
    if (title.textContent !== monthName.format(first)) title.textContent = monthName.format(first);
    scope.querySelector(".cal-nav:last-child").ariaDisabled = month >= now.slice(0, 7);
    const heads = scope.querySelector(".cal-week").children;
    grid.querySelectorAll("button").forEach((cell, i) => {
      const day = iso(start + i * DAY);
      const name = dayName.format(start + i * DAY).replace(",", ""); // "Thursday 19 November 2026"
      // A range without an end shows up to today.
      const between = (lo || hi) && (!lo || day > lo) && (hi ? day < hi : day <= now);
      if (i < 7) heads[i].textContent = name.slice(0, 3);
      cell.dataset.day = day;
      cell.textContent = +day.slice(8);
      cell.className = `cal-day${day.startsWith(month) ? "" : " out"}${between ? " in" : ""}` +
        `${day === lo ? " lo" : ""}${day === hi ? " hi" : ""}`;
      cell.ariaLabel = between ? `${name}, in range` : name;
      cell.ariaSelected = day === from || day === to;
      cell.ariaCurrent = day === now ? "date" : null;
      cell.ariaDisabled = day > now;
      cell.tabIndex = day === cal.at ? 0 : -1;
    });
  }

  // move puts the tab stop on a day, today at the latest, and from the
  // keyboard the focus too.
  function move(day, keys) {
    cal.at = day > today() ? today() : day;
    if (keys && cal.first) cal.over = cal.at;
    draw();
    if (keys) document.querySelector(".cal-day[tabindex='0']").focus();
  }

  // changed shows the fields in the calendar. What one refused is checked again.
  function changed(sel, day) {
    for (const field of sel.querySelectorAll("[data-bound]")) field.setCustomValidity("");
    move(day ?? cal.at);
  }

  // A click on a day starts a range, of that day. A second click ends it
  // there, before or after the first, and a third starts again.
  on("click", (event) => {
    const hit = cal && event.target.closest?.(".cal-day, .cal-nav");
    if (!hit || hit.ariaDisabled === "true") return;
    const { day } = hit.dataset;
    if (!day) {
      move(months(cal.at, +hit.dataset.calStep));
      return;
    }

    const sel = hit.closest(".sel");
    const days = [cal.first || day, day].sort();
    for (const [i, name] of ["since", "before"].entries()) {
      const [date, time] = boundOf(sel, name);
      date.value = days[i];
      time.value = i ? "23:59" : "00:00";
    }

    cal.first = cal.first ? null : day;
    cal.over = null;
    changed(sel, day);
  });

  on("pointerover", (event) => {
    if (!cal?.first) return;
    const day = event.target.closest?.(".cal-day[aria-disabled=false]")?.dataset.day ?? null;
    if (day === cal.over) return;
    cal.over = day;
    draw();
  });

  // The arrow keys move by a day and by a week, Home and End to the ends of
  // the week, and Page Up and Page Down by a month, or with Shift by a year.
  on("keydown", (event) => {
    const day = event.target.closest?.(".cal-day")?.dataset.day;
    if (!day || event.altKey || event.ctrlKey || event.metaKey) return;
    const at = Date.parse(day);
    const column = (new Date(at).getUTCDay() - weekStart + 7) % 7;
    const days = { ArrowLeft: -1, ArrowRight: 1, ArrowUp: -7, ArrowDown: 7, Home: -column, End: 6 - column }[event.key];
    const turn = { PageUp: -1, PageDown: 1 }[event.key];
    if (days == null && !turn) return;
    event.preventDefault();
    move(turn ? months(day, turn * (event.shiftKey ? 12 : 1)) : iso(at + days * DAY), true);
  });

  // A date that the reader empties takes its time with it: no bound.
  on("input", (event) => {
    const field = event.target;
    if (!field.matches?.("[data-bound]")) return;
    const sel = field.closest(".sel");
    const [date, time] = boundOf(sel, field.dataset.bound);
    if (field === date && !date.value) time.value = "";
    cal.first = null;
    changed(sel, dayOf(date.value));
  });

  // A bound that the reader typed shows as the dashboard reads it.
  on("change", (event) => {
    const field = event.target;
    if (!field.matches?.("[data-bound]")) return;
    asked(field.closest(".sel"), field.dataset.bound);
    draw();
  });

  // ==== Times ====

  // The server words every time in UTC, with its instant in data-time, and
  // localise words it again in the device's time zone: the device knows when
  // its zone changes its clocks. It changes a page before the page shows, so
  // an update finds the words that it already shows, and touches nothing.
  //
  // A relative time, "8s ago", counts from the page's time of rendering, in
  // data-now, second by second, in the server's words and by its clock: the
  // device's may be minutes off. The time since the page began to arrive is
  // added by the monotonic clock, or by the device's if that counted more,
  // as it does where the other stops in sleep.
  let serverOffset = NaN; // the server's clock, less the monotonic one
  let deviceOffset = NaN; // and less the device's
  let beat = 0; // the timer of the next second

  const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const ZONE = new Intl.DateTimeFormat().resolvedOptions().timeZone || "local time";
  const pad = (n, width = 2) => String(n).padStart(width, "0");
  const dateOf = (at) => `${pad(at.getFullYear(), 4)}-${pad(at.getMonth() + 1)}-${pad(at.getDate())}`;
  const clockOf = (at) => `${pad(at.getHours())}:${pad(at.getMinutes())}${at.getSeconds() ? `:${pad(at.getSeconds())}` : ""}`;

  // zoneOf names the zone of a time by how far it is from UTC at that time:
  // "UTC", "UTC+5", "UTC-4", or "UTC+5:45".
  function zoneOf(at) {
    const minutes = -Math.round(at.getTimezoneOffset());
    const rest = Math.abs(minutes) % 60;
    return minutes ? `UTC${minutes < 0 ? "-" : "+"}${Math.trunc(Math.abs(minutes) / 60)}${rest ? `:${pad(rest)}` : ""}` : "UTC";
  }

  function localise(scope) {
    for (const el of scope.querySelectorAll("[data-time]")) {
      const at = new Date(el.dataset.time);
      // No time, or a year that the browser does not read: the server's
      // words stay. So do the browser's fields, for a page without the script.
      if (Number.isNaN(at.getTime()) || el.localName === "input") continue;
      const time = `${pad(at.getHours())}:${pad(at.getMinutes())}:${pad(at.getSeconds())}.${pad(at.getMilliseconds(), 3)}`;
      el.title = `${dateOf(at)} ${time} ${zoneOf(at)}`;
    }

    // The range of the filter by time, as the server words it: in short on
    // the button, and in full, with the years and the zones, in its tooltip
    // and for a screen reader. Its end is the last minute before "before",
    // or the last second, as the fields have it.
    const sel = scope.querySelector(".sel-range");
    if (!sel) return;
    const [since, before] = [...sel.querySelectorAll("input[data-time]")]
      .map((field) => field.dataset.time && new Date(field.dataset.time));
    const whole = before && !before.getSeconds();
    const until = before && new Date(before - (whole ? 60000 : 1000));
    const clock = (at) => `${clockOf(at)}${at === until && !whole && !at.getSeconds() ? ":00" : ""}`;
    const year = new Date().getFullYear();
    const day = (at, always) =>
      `${at.getDate()} ${MONTHS[at.getMonth()]}${always || at.getFullYear() !== year ? ` ${pad(at.getFullYear(), 4)}` : ""}`;
    const brief = (at) => `${day(at)} ${clock(at)}`;
    const long = (at) => `${day(at, true)} ${clock(at)} ${zoneOf(at)}`;
    const words = (from, to) => (since && before ? `${from(since)} to ${to(until)}`
      : since ? `Since ${from(since)}` : before ? `Until ${from(until)}` : "Any time");
    const summary = sel.querySelector("summary");
    const full = words(long, long);
    summary.querySelector(".sel-label").textContent =
      words(brief, (at) => (dateOf(at) === dateOf(since) ? clock(at) : brief(at)));
    summary.setAttribute("aria-label", `${sel.querySelector(".pop").getAttribute("aria-label")}: ${full}`);
    if (since || before) summary.title = full;
    sel.querySelector("[data-zone]").textContent = ZONE;
    for (const [name, at] of [["since", since], ["before", until]]) {
      const [date, time] = boundOf(sel, name);
      date.setAttribute("value", at ? dateOf(at) : "");
      time.setAttribute("value", at ? clock(at) : "");
    }

    if (cal) draw(sel);
  }

  // syncServerClock takes the server's time from a page that began to arrive at since.
  function syncServerClock(main, since) {
    serverOffset = Date.parse(main.dataset.now) - since;
    deviceOffset = serverOffset + performance.now() - Date.now();
  }
  const serverNow = () => Math.max(serverOffset + performance.now(), deviceOffset + Date.now());

  // ago words a number of seconds as ago in funcs.go does.
  function ago(s) {
    const d = Math.abs(s);
    const n = d < 60 ? `${d}s` : d < 3600 ? `${Math.trunc(d / 60)}m`
      : d < 172800 ? `${Math.trunc(d / 3600)}h` : `${Math.trunc(d / 86400)}d`;
    return !d ? "now" : s < 0 ? `in ${n}` : `${n} ago`;
  }

  // span words the seconds of a gap that counts as span in funcs.go does.
  function span(s) {
    s = Math.max(0, s);
    const [n, unit, rest, sub] = s < 60 ? [s, "s", 0] : s < 3600 ? [s / 60, "m", s % 60, "s"]
      : s < 172800 ? [s / 3600, "h", (s / 60) % 60, "m"] : [s / 86400, "d", (s / 3600) % 24, "h"];
    return `${Math.trunc(n)}${unit}${Math.trunc(rest) ? ` ${Math.trunc(rest)}${sub}` : ""}`;
  }

  // count words the relative times of a page, and its gaps that last until
  // now, from data-since, for the clock's second; data-now says the second
  // of their words. A relative time is the end of the text of an element
  // with a time, if it is that time's words: "paused" stays.
  function count(main) {
    const from = Math.floor(Date.parse(main.dataset.now) / 1000);
    const to = Math.floor(serverNow() / 1000);
    if (!(to - from)) return; // the same second, or no time of rendering
    for (const el of main.querySelectorAll("[data-time], [data-since]")) {
      const [time, words] = el.dataset.since ? [el.dataset.since, span] : [el.dataset.time, ago];
      const at = Math.floor(Date.parse(time) / 1000);
      const was = words(from - at);
      const now = words(to - at);
      const text = el.lastChild;
      if (was !== now && text?.nodeValue?.endsWith(was)) text.nodeValue = text.nodeValue.slice(0, -was.length) + now;
    }

    main.dataset.now = new Date(to * 1000).toISOString();
  }

  // tick counts at each second: one timer, which rests in a hidden tab. A
  // text that changes ends a selection in it, so the times wait for one.
  function tick() {
    clearTimeout(beat);
    const now = serverNow();
    if (document.hidden || Number.isNaN(now)) return;
    if (window.getSelection()?.isCollapsed !== false) count(document.getElementById("main"));
    showRetry();
    beat = setTimeout(tick, 1000 - (now % 1000));
  }

  // ==== Hints and tooltips ====

  // A hint opens beside its button, toward the middle of the window, where
  // it has room, and scrolls with the page. Its place, in --inset, which a
  // phone's style overrides, is set before it shows and after an update. A
  // worker's lists are a hint with up to three buttons: it opens beside the
  // one that the browser says opened it.
  let hint = null; // the hint that opened last
  let beside = null; // the button that opened it, if the browser said

  function placeHint() {
    const at = (beside?.isConnected ? beside : hint.previousElementSibling).getBoundingClientRect();
    const down = at.top < root.clientHeight / 2;
    const forward = at.left < root.clientWidth / 2;
    const [top, bottom] = down ? [`${at.bottom + scrollY + 6}px`, "auto"]
      : ["auto", `${root.clientHeight - at.top - scrollY + 6}px`];
    const [left, right] = forward ? [`${at.left + scrollX}px`, "auto"] : ["auto", `${root.clientWidth - at.right - scrollX}px`];
    hint.style.setProperty("--inset", `${top} ${right} ${bottom} ${left}`);
  }

  on("beforetoggle", (event) => {
    const pop = event.target;
    if (event.newState !== "open" || !pop.matches?.(".hint-pop")) return;
    hint = pop;
    beside = event.source;
    placeHint();
  }, true);

  // The browser closes a hint on a click elsewhere, but not every browser
  // takes a tap on plain text for a click. A press on a button of the hint
  // is not elsewhere: the click that follows closes it.
  on("pointerdown", (event) => {
    if (hint?.matches(":popover-open") && !hint.parentElement.contains(event.target) &&
      event.target.closest?.("[popovertarget]")?.popoverTargetElement !== hint) hint.hidePopover();
  }, true);

  // The browser shows a title only after a wait. The page shows it at once,
  // in a tooltip of its own, for what is under the pointer or has the
  // keyboard's focus: the nearest title, but not one that only repeats text
  // that shows whole, or else the whole text of what an ellipsis cuts short.
  // The tooltip holds the titles, which the browser would show too; a screen
  // reader hears them as before, in aria-description.

  const TIP_GAP = 6; // between the tooltip and its element
  const tooltip = document.createElement("div");
  tooltip.className = "tooltip";
  tooltip.popover = "manual"; // above everything, an open hint too
  tooltip.setAttribute("aria-hidden", "true");
  let tipFor = null; // what the tooltip was last asked about
  let described = null; // the element that it is about
  let held = []; // the titles that it took, [element, title], the nearest first
  let resting = false; // the pointer is on the tooltip, away from its element

  const nearest = (el, test) => {
    while (el && !test(el)) el = el.parentElement;
    return el;
  };
  const cuts = (el) => el.scrollWidth > el.clientWidth && getComputedStyle(el).textOverflow === "ellipsis";

  // describe shows the tooltip of target, inside the window: over its
  // element, or under it where there is no room. With no target, or nothing
  // to say, it hides the tooltip.
  function describe(target) {
    for (const [el, title] of held) {
      if (!el.hasAttribute("title")) el.title = title; // one that it got meanwhile is newer
      el.removeAttribute("aria-description");
    }

    held = [];
    tipFor = target;
    const box = nearest(target, cuts);
    described = nearest(target, (el) => el.title) ?? box;
    for (let el = described; el; el = el.parentElement) {
      if (!el.title) continue;
      held.push([el, el.title]);
      el.setAttribute("aria-description", el.title);
      el.removeAttribute("title");
    }

    const title = described && held[0]?.[0] === described ? held[0][1] : "";
    // innerText is the text as it shows. Words that only a screen reader
    // reads are on lines of their own in it, and complete no title.
    let text = title;
    if (!title) text = described?.innerText ?? "";
    else if (!box && described.innerText?.includes(title)) text = "";
    tooltip.textContent = text;
    if (!tooltip.textContent) {
      tooltip.remove();
      return;
    }

    if (!tooltip.isConnected) {
      document.body.append(tooltip);
      tooltip.showPopover?.();
    }

    const at = described.getBoundingClientRect();
    // Its text starts where the element's does, after the padding of each.
    const x = Math.min(at.left + parseFloat(getComputedStyle(described).paddingLeft) - 9,
      root.clientWidth - tooltip.offsetWidth - 8);
    const y = at.top - tooltip.offsetHeight - TIP_GAP;
    tooltip.style.transform = `translate(${Math.round(Math.max(8, x))}px, ${Math.round(y < 8 ? at.bottom + TIP_GAP : y)}px)`;
  }

  // The tooltip goes with a press, a key, a scroll, and the focus, and when
  // the pointer leaves the window.
  const undescribe = () => {
    resting = false;
    if (tipFor) describe(null);
  };
  for (const type of ["pointerdown", "keydown", "focusout", "scroll"]) on(type, undescribe, true);
  root.addEventListener("pointerleave", undescribe);

  // The pointer may cross to the tooltip and rest on it, to read it: over the
  // tooltip, or the gap to its element, the tooltip stays. A press goes
  // through it to what lies under it, so the page asks where the pointer is,
  // and only while it is there does it ask at each move.
  const onTip = ({ clientX: x, clientY: y }) => {
    const box = tooltip.isConnected && tooltip.getBoundingClientRect();
    return box && x >= box.left && x <= box.right && y >= box.top - TIP_GAP - 1 && y <= box.bottom + TIP_GAP + 1;
  };
  on("pointerover", (event) => {
    if (event.pointerType === "touch" || event.buttons) return;
    resting = onTip(event);
    if (!resting) describe(event.target);
  });
  on("pointermove", (event) => {
    if (resting && !onTip(event)) {
      resting = false;
      describe(event.target);
    }
  });
  // The focus may scroll the page to its element, which hides the tooltip:
  // it shows after that, in the same frame.
  on("focusin", ({ target }) => {
    requestAnimationFrame(() => {
      if (target.matches?.(":focus-visible")) describe(target);
    });
  });
  // A finger cannot hover: a tap on text that leads nowhere shows the
  // tooltip, until the next tap.
  on("click", ({ target }) => {
    if (coarse.matches && target.closest?.("a, button, summary, label, [data-href]") === null) describe(target);
  });

  // ==== Charts ====

  // A guide marks the step of a chart under the pointer or a pressed finger,
  // or that the chart's slider is on, and a note beside it gives the step's
  // time and values, which the page holds: each series has its values as
  // text, and the legend names them. The guide is an element in the chart,
  // so it scrolls and resizes with it. An update removes it, as whatever
  // the server did not render, and draws it again before the browser paints.

  const guide = document.createElement("div");
  guide.className = "chart-guide";
  guide.id = "chart-guide"; // a name of its own: an update never takes it for a part of the page
  guide.setAttribute("aria-hidden", "true"); // the chart's slider says the step
  const tip = guide.appendChild(document.createElement("div"));
  tip.className = "chart-tip";
  let traced = null; // the chart with the guide, and the pointer's place or its slider's step: { svg, x } or { svg, i }
  let marked = 0; // the step that the guide marks
  let tracing = 0; // the frame that will draw the guide: pointer moves between two frames draw once

  function untrace() {
    traced = null;
    guide.remove();
  }

  function trace() {
    tracing = 0;
    const svg = traced?.svg;
    const series = svg?.isConnected ? [...svg.querySelectorAll("[data-values]")].map((g) => g.dataset.values.split(" ")) : [];
    if (series.length === 0) { // the reader left the chart, or its page
      untrace();
      return;
    }

    const steps = series[0].length;
    const box = svg.getBoundingClientRect();
    const i = Math.min(steps - 1, Math.max(0, traced.i ?? Math.floor(((traced.x - box.left) / box.width) * steps)));
    const plot = svg.parentNode;
    if (i === marked && guide.parentNode === plot) return; // still on the step that it marks
    marked = i;
    const step = 1000 * svg.dataset.step;
    const start = 1000 * svg.dataset.start + i * step;
    const time = document.createElement("span");
    time.className = "chart-tip-time";
    // The time of the step, on the reader's clock, with its zone as every
    // exact time of the page names it.
    const from = new Date(start);
    time.textContent = `${clockOf(from)}${step > 60000 ? ` to ${clockOf(new Date(start + step))}` : ""} ${zoneOf(from)}`;
    tip.replaceChildren(time, ...[...svg.closest(".chart").querySelectorAll(".legend span")].flatMap((name, k) => {
      const value = document.createElement("b");
      value.textContent = series[k]?.[i] || "—";
      return [name.cloneNode(true), value];
    }));
    guide.classList.toggle("flip", 2 * i >= steps); // the note stays inside the plot
    // The guide is the left edge of a box as wide as the plot.
    guide.style.transform = `translateX(${((i + 0.5) / steps) * 100}%)`;
    plot.append(guide);
  }

  for (const type of ["pointerdown", "pointermove"]) {
    on(type, (event) => {
      const svg = event.target.closest?.(".chart-plot > svg");
      if (!svg) return;
      traced = { svg, x: event.clientX };
      tracing ||= requestAnimationFrame(trace);
    });
  }
  // The pointer left the chart, or the finger lifted.
  on("pointerout", (event) => {
    if (traced?.svg.contains(event.target) && !traced.svg.contains(event.relatedTarget)) untrace();
  });

  // A chart has a slider for the keyboard and a screen reader: a range field
  // over the plot, which nobody sees. The browser moves it, from the newest
  // step, and the guide follows it while it has the focus; Escape hides the
  // guide. say gives the slider, as its value, the words of its step: the
  // tooltip of the step's column.
  function say(slider) {
    const words = slider.previousElementSibling.querySelectorAll(".col")[slider.value - 1]?.textContent ?? "";
    if (slider.getAttribute("aria-valuetext") !== words) slider.setAttribute("aria-valuetext", words);
  }

  for (const type of ["focusin", "input"]) {
    on(type, (event) => {
      const slider = event.target;
      if (!slider.matches?.(".chart-step")) return;
      say(slider);
      traced = { svg: slider.previousElementSibling, i: slider.value - 1 };
      trace();
    });
  }
  on("focusout", (event) => {
    if (traced?.svg.nextElementSibling === event.target) untrace();
  });
  on("keydown", (event) => {
    if (event.key === "Escape" && event.target.matches?.(".chart-step")) untrace();
  });

  // ==== Conveniences ====

  // The theme: System, Light, or Dark.
  on("click", (event) => {
    const choice = event.target.closest?.("[data-theme-option]");
    if (choice) window.TaskmillTheme?.set(choice.dataset.themeOption);
  });
  window.TaskmillTheme?.apply();

  on("keydown", (event) => {
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

  on("click", async (event) => {
    const button = event.target.closest?.("[data-copy]");
    const source = button && document.querySelector(button.dataset.copy);
    if (!source) return;
    const text = button.lastElementChild;
    const icon = button.querySelector("use");
    try {
      await navigator.clipboard.writeText(source.textContent);
      text.textContent = "Copied";
      icon.setAttribute("href", "#i-check");
    } catch {
      text.textContent = "Select and copy";
    }

    setTimeout(() => {
      text.textContent = "Copy";
      icon.setAttribute("href", "#i-copy");
    }, 1600);
  });

  // A payload shows its JSON in colors.
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
        const mark = document.createElement("span");
        if (match[1]) {
          mark.className = match[2] ? "j-key" : "j-str";
          mark.textContent = match[1];
          out.append(mark, match[2] || "");
        } else {
          mark.className = match[3] ? "j-num" : "j-lit";
          mark.textContent = match[0];
          out.append(mark);
        }
        last = match.index + match[0].length;
      }
      out.append(text.slice(last));
      pre.replaceChildren(out);
    }
  }

  // ==== Start ====

  highlight(document);
  localise(document);
  for (const slider of document.querySelectorAll(".chart-step")) say(slider);
  syncServerClock(document.getElementById("main"), performance.getEntriesByType("navigation")[0]?.responseStart || performance.now());
  if (retrying()) failed(heading(document.getElementById("main")));
  else if (live) schedule(REFRESH_MS);
  tick();
  showLiveStatus();
  // The last click listener: one before it that takes a click prevents its default.
  on("click", followLink);
})();
