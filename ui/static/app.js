// The interface works without this script. It shows another page, a filter,
// or the result of an action in place, keeps pages live, confirms actions,
// shows times in the reader's time zone and keeps them counting, and adds a
// few conveniences. It is careful with the network and the machine: one page
// request at a time, each abandoned after a while. While the server fails or
// is out of reach, the page says so, and tries again less often after each
// failure. A page that is not the dashboard's, such as a login page, the
// browser loads.
//
// The script is this module and the modules that it imports, each a part:
// core.js, what the others share; morph.js; navigation.js; live.js;
// dialog.js and actions.js; menus.js; calendar.js; times.js; tips.js; and
// charts.js. The page loads them from a folder named after their version,
// so an import such as "./morph.js" gets the same version. A part adds no
// listener as it loads: this module starts the parts, in order.

import * as actions from "./actions.js";
import * as calendar from "./calendar.js";
import * as charts from "./charts.js";
import { on, root } from "./core.js";
import * as live from "./live.js";
import * as menus from "./menus.js";
import { highlight } from "./morph.js";
import * as navigation from "./navigation.js";
import { localise, syncServerClock } from "./times.js";
import * as tips from "./tips.js";

// The parts add their listeners here, never as they load, so this file shows
// the order in which the listeners of one event run.
navigation.listen();
live.listen();
actions.listen();
actions.showNotice();
menus.listen();
calendar.listen();
tips.listen();
charts.listen();

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

// The page that the browser loaded.
highlight(document);
localise(document);
calendar.localiseRange(document);
for (const slider of document.querySelectorAll(".chart-step")) charts.say(slider);
syncServerClock(document.getElementById("main"), performance.getEntriesByType("navigation")[0]?.responseStart || performance.now());
live.start();
// The last click listener: one before it that takes a click prevents its default.
on("click", navigation.followLink);

// Every part started: theme.js keeps the controls that need the script.
root.classList.add("js-started");
