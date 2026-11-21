// Every few seconds the page fetches itself and updates in place, unless
// the reader is busy with it. The Live button says whether the page is
// live, paused, out of date, or signed out; its dot pulses while updates
// run. While the server fails, the page on screen stays. A page that says
// the server failed is live too: it says when it tries again, and gives
// way to the page once the server answers.
//
// A live update shows its page through apply and render, in navigation.js,
// which imports this module back: a page that shows starts its updates, and
// says whether it is live. They call each other only once every module has
// loaded.

import { assets, assetsOf, inflight, load, on } from "./core.js";
import { dialog, submitting } from "./dialog.js";
import { apply, entry, render, shownETag } from "./navigation.js";
import { count, serverNow } from "./times.js";
import { describe, described, tipFor } from "./tips.js";

const REFRESH_MS = 5000; // between live updates
const MAX_WAIT_MS = 60000; // the longest wait between failed updates: 5, 10, 20, 40, then 60 seconds
const LIVE_KEY = "taskmill-live";

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
let beat = 0; // the timer of the next second: see tick

export const livePage = () => document.getElementById("main")?.hasAttribute("data-live") ?? false;
const retrying = () => document.getElementById("main")?.dataset.live === "retry";

export function showLiveStatus() {
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
export function signOut() {
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

// The reader is busy during a press, an action, or a scroll, while a
// dialog or a menu is open or text is selected, and while a field has the
// focus: also the slider of a chart, whose steps must not change under it.
function busy() {
  if (submitting || pointerDown || dialog?.open || document.querySelector("details.sel[open]")) return true;
  if (Date.now() - scrolledAt < 1000) return true;
  if (document.activeElement?.matches("input:not([type=checkbox]), select, textarea")) return true;
  return window.getSelection()?.isCollapsed === false;
}

export function schedule(ms) {
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
export function startUpdates(result) {
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

// listen listens to the reader, who may be busy with the page, to the Live
// button, and to the tab and the network, which come and go.
export function listen() {
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
}

// start starts the updates of the page that the browser loaded, as
// startUpdates does for a page that the script shows, and the clock.
export function start() {
  if (retrying()) failed(heading(document.getElementById("main")));
  else if (live) schedule(REFRESH_MS);
  tick();
  showLiveStatus();
}
