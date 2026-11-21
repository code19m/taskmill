// Multiple choice filters: menus of checkboxes that apply as they change.
//
// A filter shows its page through navigate, and a page that shows searches
// an open menu again (refine): this module and navigation.js import each
// other. Each calls the other only once every module has loaded.

import { asked, boundOf, openCalendar } from "./calendar.js";
import { coarse, on, timeOut } from "./core.js";
import { navigate } from "./navigation.js";

const SEARCH_WAIT_MS = 250; // typing this close asks the server once

let lookup = null; // a search on the server
let lookupTimer = 0;

export const openMenus = () => document.querySelectorAll("details.sel[open]");
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
export function shut(sel) {
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
export function refine(sel) {
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

// listen listens to the menus: they open, close, search, and apply as the
// reader uses them.
export function listen() {
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
}

// applyFilters shows the page for the filters at once. A change made while
// the last one loads abandons it, so quick clicks cost no more than one.
// A range of time that is not one stays: its field says so, not a page
// of its own, and applyFilters returns false.
export function applyFilters(form) {
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
