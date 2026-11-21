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

export const pad = (n, width = 2) => String(n).padStart(width, "0");
export const dateOf = (at) => `${pad(at.getFullYear(), 4)}-${pad(at.getMonth() + 1)}-${pad(at.getDate())}`;
export const clockOf = (at) => `${pad(at.getHours())}:${pad(at.getMinutes())}${at.getSeconds() ? `:${pad(at.getSeconds())}` : ""}`;

// zoneOf names the zone of a time by how far it is from UTC at that time:
// "UTC", "UTC+5", "UTC-4", or "UTC+5:45".
export function zoneOf(at) {
  const minutes = -Math.round(at.getTimezoneOffset());
  const rest = Math.abs(minutes) % 60;
  return minutes ? `UTC${minutes < 0 ? "-" : "+"}${Math.trunc(Math.abs(minutes) / 60)}${rest ? `:${pad(rest)}` : ""}` : "UTC";
}

export function localise(scope) {
  for (const el of scope.querySelectorAll("[data-time]")) {
    const at = new Date(el.dataset.time);
    // No time, or a year that the browser does not read: the server's
    // words stay. So do the browser's fields, for a page without the script.
    if (Number.isNaN(at.getTime()) || el.localName === "input") continue;
    const time = `${pad(at.getHours())}:${pad(at.getMinutes())}:${pad(at.getSeconds())}.${pad(at.getMilliseconds(), 3)}`;
    el.title = `${dateOf(at)} ${time} ${zoneOf(at)}`;
  }
}

// syncServerClock takes the server's time from a page that began to arrive at since.
export function syncServerClock(main, since) {
  serverOffset = Date.parse(main.dataset.now) - since;
  deviceOffset = serverOffset + performance.now() - Date.now();
}
export const serverNow = () => Math.max(serverOffset + performance.now(), deviceOffset + Date.now());

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
export function count(main) {
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
