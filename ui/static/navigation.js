// Pages in place. A link, a filter, an action, and the back and forward
// buttons show their page without loading it anew, and a live update
// changes the page on screen in place: apply and render.
//
// live.js and menus.js import this module, and it imports them back: a page
// that shows starts its live updates, and an update shows a page; a filter
// shows a page, and a page that shows searches an open menu again. They call
// each other only once every module has loaded.

import { localiseRange } from "./calendar.js";
import { say, trace, traced, untrace } from "./charts.js";
import { assets, assetsOf, inflight, load, loadWhole, notify, page, progress, SLOW_MS } from "./core.js";
import { dialog, release, showDialogPart } from "./dialog.js";
import { livePage, schedule, showLiveStatus, startUpdates } from "./live.js";
import { openMenus, placeMenu, refine, shut } from "./menus.js";
import { highlight, morph, syncAttributes } from "./morph.js";
import { count, localise } from "./times.js";
import { describe, hint, placeHint, tipFor, undescribe } from "./tips.js";

const FADE_MS = 140; // another page fades in
const KEEP_PAGES = 10; // pages kept in memory for the back and forward buttons
const reducedMotion = matchMedia("(prefers-reduced-motion: reduce)");

// Each entry of the history gets a number, and the page that it showed is
// kept under that number while the tab lives, with its scroll position.
// So the back and forward buttons show a page right away, where the
// reader left it.
const pages = new Map();
const stamp = () => Date.now() + Math.random();
const here = () => location.pathname + location.search;
export let entry = history.state?.n; // of the page on screen
let onScreen = here(); // the address of the page on screen
export let turns = 0; // counts navigations and actions: a newer one takes over
if (entry == null) {
  entry = stamp();
  history.replaceState({ n: entry }, "");
}

// nextTurn starts a navigation or an action, and returns its number.
export const nextTurn = () => ++turns;

export let shownETag = null; // of the page on screen, once an update fetched it

// apply changes the page on screen into the one of result. A newer state
// of the same page changes only where it differs. Another page, fresh,
// replaces it whole: nothing of the reader's state belongs to it.
export function apply(result, fresh = false) {
  // The page changes with its titles on it. Then the tooltip shows again.
  const about = tipFor;
  const focused = document.activeElement;
  const place = placeOf(focused);
  undescribe();
  localise(result.main);
  localiseRange(result.main);
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
  // In a row that stays, the control in its place takes it.
  if (focused?.isConnected === false) (inPlace(main, place) ?? main).focus({ preventScroll: true });
  if (result.doc.title) document.title = result.doc.title;
  shownETag = result.etag;
  highlight(main);
  for (const slider of main.querySelectorAll(".chart-step")) say(slider);
  if (traced) trace(); // the update took the guide off its chart, or the chart off the page
  if (hint?.matches(":popover-open")) placeHint();

  // The newer copy lists every option of a menu, and none that a search
  // found on the server: an open menu shows its search again. Its copy has
  // none of the classes that placed it: it is placed again.
  for (const sel of openMenus()) {
    refine(sel);
    placeMenu(sel);
  }
  if (about?.isConnected) describe(about);
}

// placeOf returns the place of a control in a row of a table: the row's key,
// the cell, and the control's place in the cell, counted from its end. A
// toggle such as Pause ends its cell, and the controls before it come and
// go, as Run now does while a schedule is paused.
function placeOf(control) {
  const cell = control?.closest?.("tr[data-key] > td");
  if (!cell) return null;
  const controls = [...cell.querySelectorAll("a[href], button")];
  return { key: cell.parentElement.dataset.key, cell: cell.cellIndex, fromEnd: controls.length - 1 - controls.indexOf(control) };
}

// inPlace returns the control at place in main, or the one nearest to it in
// the same cell, as Resume takes the place of Pause.
function inPlace(main, place) {
  if (!place) return null;
  const row = main.querySelector(`tr[data-key="${CSS.escape(place.key)}"]`);
  const controls = [...(row?.cells[place.cell]?.querySelectorAll("a[href], button") ?? [])];
  return controls[Math.max(controls.length - 1 - place.fromEnd, 0)];
}

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
// does, and "replace" changes this one, as a filter does. Safari throws
// when a page changes its address too often: the browser loads the page
// then, and address returns nothing.
export function address(url, mode) {
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
// before an action.
export async function navigate(url, mode) {
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
export function render(result, { moved, n, y = 0, notice = null }) {
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

// listen listens to the back and forward buttons. app.js adds followLink
// last of all.
export function listen() {
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
}

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
export function followLink(event) {
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

  // A link in a menu, a preset of the range of time, is a filter too, as
  // is a period of the charts. The one choice closes the menu.
  const sel = hit?.closest("details.sel");
  if (sel) shut(sel);
  const asks = hit?.matches("a[data-dialog]") && typeof dialog?.showModal === "function";
  navigate(url, more || sel || hit?.closest("[data-filter]") ? "replace" : asks ? "ask" : "push");
}
