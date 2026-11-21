// What the other parts share: the page's settings and its events, the
// loading bar and the notices, and how a page is fetched: one request at a
// time, each abandoned after a while.

import { syncServerClock } from "./times.js";

const TIMEOUT_MS = 15000; // then a request is abandoned: after the server's 10 seconds, whose page says so
export const SLOW_MS = 300; // a navigation that takes longer shows progress
export const NOTICE_MS = 4000; // a notice stays this long

export const root = document.documentElement;
export const coarse = matchMedia("(pointer: coarse)");

// The static files of this page. A page from a newer server has others,
// and then the whole page reloads, so that new markup never meets old code.
export const assetsOf = (doc) => [...doc.querySelectorAll("head script[src], head link[rel=stylesheet]")]
  .map((node) => node.getAttribute("src") || node.getAttribute("href")).join(" ");
export const assets = assetsOf(document);

// While the next page loads, a bar slides under the header and the page it
// replaces fades back.
export const progress = (shown) => root.classList.toggle("loading", shown);

// on listens to an event anywhere on the page.
export const on = (type, listener, options) => document.addEventListener(type, listener, options);

// loadWhole has the browser load a page, as it does without the script.
export function loadWhole(url) {
  location.assign(url);
  progress(true);
}

export let inflight = null; // the page request in flight: see load

// page reads a page of the interface from its HTML. It returns nothing
// for other HTML, such as a login page.
export function page(html) {
  const doc = new DOMParser().parseFromString(html, "text/html");
  const main = doc.getElementById("main");
  return main ? { kind: "page", doc, main } : null;
}

// timeOut abandons the request of control after TIMEOUT_MS, and returns
// what stops the wait. The abort's reason says that time ran out.
export function timeOut(control) {
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
export async function request(url, { etag = null, body = null, control = new AbortController() } = {}) {
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
export async function load(url, etag) {
  inflight?.abort();
  const control = new AbortController();
  inflight = control;
  try {
    return await request(url, { etag, control });
  } finally {
    if (inflight === control) inflight = null;
  }
}

// notify shows a notice in place of the one before it, for a while. The
// time counts once the tab is in view: a notice must not come and go while
// the reader is elsewhere.
export function notify(toast, ms = NOTICE_MS) {
  for (const old of document.querySelectorAll(".toast")) if (old !== toast) old.remove();
  if (!document.contains(toast)) document.querySelector(".main").before(toast); // from the page that was fetched
  const start = () => {
    setTimeout(() => toast.classList.add("leaving"), ms);
    setTimeout(() => toast.remove(), ms + 400);
  };
  if (document.hidden) on("visibilitychange", start, { once: true });
  else start();
}
