// The filter by time: a calendar, and two bounds to type. To is the last
// minute of the range: the address takes the start of the next one, as
// "before". So a range of days ends at 23:59, with all of its last day. A
// day is its date as text, "2026-11-19". The calendar counts days in UTC:
// only a bound is read on the reader's clock.

import { on } from "./core.js";
import { clockOf, dateOf, pad, zoneOf } from "./times.js";

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const ZONE = new Intl.DateTimeFormat().resolvedOptions().timeZone || "local time";
const DAY = 864e5;
const iso = (ms) => new Date(ms).toISOString().slice(0, 10);
const today = () => dateOf(new Date());
export const boundOf = (sel, name) => sel.querySelectorAll(`[data-bound=${name}]`); // its date, and its time
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
export function openCalendar(sel) {
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
export function asked(sel, name) {
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

// rangeWords words the range from since to before, either of which may be
// missing, as the server words it, in the device's time zone: in short, and
// in full, with the years and the zones. Its end is until, the last minute
// before "before", or the last second, as the fields have it, and clock
// reads the time of a bound as the words do.
function rangeWords(since, before) {
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
  const short = words(brief, (at) => (dateOf(at) === dateOf(since) ? clock(at) : brief(at)));
  return { short, full: words(long, long), until, clock };
}

// localiseRange words the ranges of time in scope as the server words them,
// in the device's time zone, as localise does a time. The filter by time
// has its range in short on the button, and in full in its tooltip and for
// a screen reader; the page of a bulk action names its range in full. An
// open calendar is drawn in scope too, as draw says.
export function localiseRange(scope) {
  const timeOf = (instant) => instant && new Date(instant);
  for (const el of scope.querySelectorAll("[data-range-since]")) {
    el.textContent = rangeWords(timeOf(el.dataset.rangeSince), timeOf(el.dataset.rangeBefore)).full;
  }

  const sel = scope.querySelector(".sel-range");
  if (!sel) return;
  const [since, before] = [...sel.querySelectorAll("input[data-time]")].map((field) => timeOf(field.dataset.time));
  const { short, full, until, clock } = rangeWords(since, before);
  const summary = sel.querySelector("summary");
  summary.querySelector(".sel-label").textContent = short;
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

// listen listens to the clicks, keys, and fields of the calendar.
export function listen() {
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
}
