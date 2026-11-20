// The interface works without this script. It keeps pages live, shows
// another page, a filter, or the result of an action without loading the
// page anew, shows times in the reader's time zone and keeps them counting,
// confirms actions, and adds a few conveniences.
//
// It is careful with the network and the machine: one page request at a
// time, each abandoned after a while, and one request for each page that
// the reader asks for. While the server fails or is out of reach, the page
// says so and tries again by itself, waiting longer after each failure, and
// shows the page once the server answers. An update changes only the parts
// of the page that changed, so nothing flickers, and scroll, open menus,
// and the focus stay as they are, unless what had the focus is gone. A page
// that is not the dashboard's, such as a login page, the browser loads.
(() => {
  "use strict";

  const REFRESH_MS = 5000; // between live updates
  const TIMEOUT_MS = 15000; // a request that takes longer is abandoned: longer than the server's 10 seconds, so that its page that says so arrives
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
  let confirming = null; // the form that the dialog asks about
  let submitting = false;

  // The notice of an action that the dashboard did not answer: what may
  // have happened. A device that was offline sent nothing, and a session
  // that ended let nothing through. Otherwise the action may have reached
  // the dashboard, and applied, before the answer was lost.
  const UNSENT = "This device is offline, so the action was not sent.";
  const REFUSED = "The session ended, so the action did not apply. Reload the page to sign in again.";
  const UNCONFIRMED = "No answer came for this action. It may have applied: check the page before you try again.";

  function failure(text) {
    const toast = document.querySelector("[data-failure]").content.firstElementChild.cloneNode(true);
    toast.querySelector(".toast-message").textContent = text;
    return toast;
  }

  // unanswered says why an action got no page in answer, and has the page
  // show what is true now, as soon as the dashboard answers.
  function unanswered(result, offline) {
    if (result.kind === "signed-out") {
      status = "signed-out";
      show();
      return REFUSED;
    }
    schedule(0);
    return offline ? UNSENT : UNCONFIRMED;
  }

  // ask shows the dialog: its title, its text, what its status line says,
  // and its main button, which reads as label and sends form. Without a
  // form, the dialog only says something, and closes.
  function ask(title, text, form, label = [], look = "", said = "") {
    const [cancel, button] = dialog.querySelectorAll("button");
    dialog.querySelector("h2").textContent = title;
    dialog.querySelector("[data-confirm-status]").textContent = said;
    dialog.querySelector("[data-confirm-text]").replaceChildren(...text);
    cancel.textContent = form ? "Cancel" : "Close";
    button.hidden = !form;
    button.className = look;
    button.replaceChildren(...label);
    // Escape closes the dialog without a value: an earlier one must not count.
    dialog.returnValue = "";
    confirming = form;
    if (!dialog.open) dialog.showModal();
    if (!form) cancel.focus(); // the main button is gone, and may have had the focus
  }

  // tell shows in the dialog what a page says there, if it has a part for
  // the dialog: what an action on many tasks would change, the next step of
  // one, or why one failed. The part names the title. Its filters and
  // sentences are the text, its form's button is the main button, and the
  // page's notice, which says what the last step did, is the status.
  function tell(result) {
    const part = result.main.querySelector("[data-dialog-title]");
    if (!part) return false;
    const form = part.querySelector("form");
    const button = form?.querySelector("button");
    ask(part.dataset.dialogTitle, part.querySelectorAll("dl, p"), form, button?.childNodes, button?.className,
      result.doc.querySelector("[data-toast]")?.textContent);
    return true;
  }

  // arrive shows the page that answered an action, in place: the page that
  // the action leads to, with its notice, or the page that says why the
  // action did not apply.
  function arrive(result) {
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
    landed(result);
  }

  // send runs the action of a form on the page, and shows the page that the
  // server answers with.
  async function send(form) {
    const buttons = [...form.querySelectorAll("button")];
    const turn = ++turns;
    form.dataset.jsPending = "";
    submitting = true;
    for (const button of buttons) button.disabled = true;
    const slow = setTimeout(() => progress(true), SLOW_MS);
    const offline = navigator.onLine === false;
    const result = await request(form.action, { body: new URLSearchParams(new FormData(form)) });
    clearTimeout(slow);
    submitting = false;
    progress(false);
    delete form.dataset.jsPending;
    for (const button of buttons) button.disabled = false;
    if (result.kind !== "page") {
      notify(failure(unanswered(result, offline)), 2 * NOTICE_MS);
      return;
    }
    if (turn !== turns) return; // the reader went on to another page meanwhile
    arrive(result);
  }

  // act runs the action that the dialog asks about for a page, and keeps the
  // dialog for the answer: the next step, or why the action failed. For the
  // page that shows the result, the dialog closes. The status line says
  // that the action runs, and if no answer arrived. A reader who closed the
  // dialog meanwhile gets the answer all the same.
  async function act() {
    if (submitting) return; // a second press sends nothing
    const said = dialog.querySelector("[data-confirm-status]");
    const turn = ++turns;
    submitting = true;
    dialog.ariaBusy = "true";
    said.textContent = said.dataset.busy;
    const offline = navigator.onLine === false;
    const result = await request(confirming.action, { body: new URLSearchParams(new FormData(confirming)) });
    submitting = false;
    dialog.ariaBusy = null;
    if (result.kind !== "page") {
      const text = unanswered(result, offline);
      if (dialog.open) said.textContent = text;
      else notify(failure(text), 2 * NOTICE_MS);
      return;
    }
    if (turn !== turns || (assetsOf(result.doc) === assets && tell(result))) return;
    dialog.close();
    arrive(result);
  }

  // release ends the pending state of every form. A page that was kept while
  // an action was on its way comes back with it.
  function release() {
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
      ask(form.dataset.confirmTitle || "Are you sure?", [form.dataset.confirm], form,
        [...(icon ? [icon.cloneNode(true)] : []), form.dataset.confirmLabel || "Confirm"],
        "confirmDanger" in form.dataset ? "btn danger-solid" : "btn primary");
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
    release();
    schedule(0);
  });

  // Multiple choice filters: menus of checkboxes that apply as they change

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

  // visible shows control characters as the server does.
  const visible = (text) => text.replace(/[\u0000-\u001f]/g, (c) => String.fromCharCode(0x2400 + c.charCodeAt(0)));

  // group shows a FIFO group key as the server's "group" does: a key that
  // the outbox made as its topic and its message key, and any other as it is.
  function group(key) {
    const parts = /^([^\u0000-\u001f]+)\u001f([^\u0000-\u001f]+)$/.exec(key);
    if (!parts) return visible(key);
    const span = (name, text) => Object.assign(document.createElement("span"), { className: name, textContent: text });
    const shown = document.createElement("span");
    const sep = span("topic-sep", "›");
    sep.ariaHidden = "true";
    shown.title = `Outbox topic ${parts[1]}, key ${parts[2]}`;
    shown.append(span("topic", parts[1]), sep, parts[2]);
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

  // A hint opens beside its button, toward the middle of the window: there
  // it has room, whatever its size. Its place is on the page, so it moves
  // with the page as that scrolls. The place is set before the hint shows,
  // and again after an update, which can move the button: the row of a
  // worker moves when another worker comes or goes. A worker's lists are a
  // hint with two buttons: where the browser says which one opened it, it
  // opens beside that one.
  let hint = null; // the hint that opened last
  let beside = null; // the button that opened it, if the browser said

  function settle() {
    const at = (beside?.isConnected ? beside : hint.previousElementSibling).getBoundingClientRect();
    const down = at.top < root.clientHeight / 2;
    const forward = at.left < root.clientWidth / 2;
    hint.style.top = down ? `${at.bottom + scrollY + 6}px` : "auto";
    hint.style.bottom = down ? "auto" : `${root.clientHeight - at.top - scrollY + 6}px`;
    hint.style.left = forward ? `${at.left + scrollX}px` : "auto";
    hint.style.right = forward ? "auto" : `${root.clientWidth - at.right - scrollX}px`;
  }

  document.addEventListener("beforetoggle", (event) => {
    const pop = event.target;
    if (event.newState !== "open" || !pop.matches?.(".hint-pop")) return;
    hint = pop;
    beside = event.source;
    settle();
  }, true);

  document.addEventListener("toggle", (event) => {
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
    // The calendar opens on the last day of the range, or on today.
    if (sel.matches(".sel-range")) {
      cal = {};
      move(dayOf(boundOf(sel, "before")[0].value) ?? today());
    }
    place(sel);
    if (!coarse.matches) (field || boxes(sel)[0])?.focus({ preventScroll: true });
  }, true);

  let pointerDown = false;
  document.addEventListener("pointerdown", (event) => {
    pointerDown = true;
    for (const sel of openMenus()) if (!sel.contains(event.target)) sel.open = false;
    // The browser closes a hint on a click elsewhere, but not every browser
    // takes a tap on plain text for a click. A press on a button of the
    // hint is not elsewhere: the click that follows closes it.
    if (hint?.matches(":popover-open") && !hint.parentElement.contains(event.target) &&
      event.target.closest?.("[popovertarget]")?.popoverTargetElement !== hint) hint.hidePopover();
  }, true);
  // A press ends when the pointer lifts. A context menu, on macOS and Linux,
  // and a window that comes to the front take the lift with them: the live
  // updates must not wait for it.
  const lift = () => { pointerDown = false; };
  for (const type of ["pointerup", "pointercancel", "contextmenu"]) document.addEventListener(type, lift, true);
  window.addEventListener("blur", lift);

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

  document.addEventListener("click", (event) => {
    const add = event.target.closest?.("[data-sel-add]");
    if (add?.dataset.value) addValue(add.closest(".sel"), add.dataset.value);
  });

  document.addEventListener("input", (event) => {
    const field = event.target;
    if (field.matches?.("[data-sel-search]")) refine(field.closest(".sel"));
    // A date that the reader empties takes its time with it: no bound.
    if (field.matches?.("[data-bound]")) {
      const sel = field.closest(".sel");
      const [date, time] = boundOf(sel, field.dataset.bound);
      if (field === date && !date.value) time.value = "";
      cal.first = null;
      changed(sel, dayOf(date.value));
    }
  });

  document.addEventListener("change", (event) => {
    const box = event.target;
    // A bound that the reader typed shows as the dashboard reads it.
    if (box.matches?.("[data-bound]")) {
      asked(box.closest(".sel"), box.dataset.bound);
      draw();
    }
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
      if (asks[0] && asks[1] && asks[0] >= asks[1]) boundOf(range, "before")[0].setCustomValidity("The range must not end before it starts.");
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

  // The history. Each entry gets a number, and the page that it showed is
  // kept under that number while the tab lives, with its scroll position.
  // So the back and forward buttons show a page right away, where the
  // reader left it.
  const pages = new Map();
  const stamp = () => Date.now() + Math.random();
  const here = () => location.pathname + location.search;
  let entry = history.state?.n; // of the page on screen
  let onScreen = here(); // the address of the page on screen
  if (entry == null) {
    entry = stamp();
    history.replaceState({ n: entry }, "");
  }

  // leave keeps the page on screen, before another one takes its place:
  // its title, its content, and the parts around it that an update changes.
  // What the script put there stays out: the guide of a chart, and the
  // place of a hint, which is a style attribute.
  function leave() {
    untrace();
    undescribe(); // the page is kept with its titles
    for (const pop of document.querySelectorAll(".hint-pop[style]")) pop.removeAttribute("style");
    const parts = [document.getElementById("main"), ...document.querySelectorAll("[data-live-part]")];
    pages.delete(entry);
    pages.set(entry, { title: document.title, html: parts.map((part) => part.outerHTML).join(""), y: scrollY });
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
  // with its place in the history as address gives it: one request for
  // each. A page that says the server failed shows in place too, and so
  // does the page that says that no answer came: each tries again by
  // itself. A page that is not the dashboard's, such as a login page, or
  // one of a newer version of it, the browser loads. In the mode "ask",
  // the dialog shows the page, which asks before an action on many tasks:
  // the page on screen stays, and so does the history.
  let turns = 0; // counts navigations and actions: a newer one takes over

  async function navigate(url, mode) {
    const turn = ++turns;
    const slow = setTimeout(() => progress(true), SLOW_MS);
    let result = await load(url.href, null);
    clearTimeout(slow);
    if (turn !== turns || result.kind === "aborted") return; // a newer change takes over
    progress(false);
    if (result.kind === "down") result = unreachable();
    if (result.kind !== "page" || assetsOf(result.doc) !== assets) {
      location.assign(url.href);
      progress(true);
      return;
    }
    if (mode === "ask" && tell(result)) return;
    const n = address(url, mode);
    if (n == null) return;
    render(result, { moved: mode !== "replace", n });
    landed(result);
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
    onScreen = here();
    show(); // the page may be one that does not update itself
    if (notice) notify(notice);
    const main = document.getElementById("main");
    const announcer = document.querySelector("[data-announce]");
    if (!moved) {
      // The change may remove the element that had focus, as the last "Load
      // more" removes its link: the page takes the focus, not the body. And
      // a list that is longer, or has other filters, tells a screen reader
      // how many tasks it shows now, or that it shows none. An action says
      // what it did itself.
      if (focused?.isConnected === false) main.focus({ preventScroll: true });
      if (!notice && announcer) announcer.textContent = main.querySelector("[data-shown], .empty-title")?.textContent ?? "";
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
    turns++; // a page or an action that is on its way was for the page before
    progress(false);
    dialog?.close?.(); // it asked about the page before
    leave();
    result.doc.title = was.title;
    render(result, { moved: true, n, y: was.y });
    release();
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
    // A link in a menu is a filter too, a preset of the range of time: the
    // one choice closes the menu.
    const sel = hit?.closest("details.sel");
    if (sel) shut(sel);
    const asks = hit?.matches("a[data-dialog]") && typeof dialog?.showModal === "function";
    navigate(url, more || sel ? "replace" : asks ? "ask" : "push");
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

  // Tooltips
  //
  // The browser shows a title only after a wait. The page shows it right
  // away, in a tooltip of its own, for what is under the pointer or has the
  // keyboard's focus: the nearest title, or else the whole text of what an
  // ellipsis cuts short. A title that only repeats text that shows whole
  // says nothing. The tooltip holds the title meanwhile, so the browser
  // shows none, and a screen reader hears it as before, in aria-description.
  // It holds the titles further out too: without its own, an element shows
  // the title of the one around it.

  const tooltip = document.createElement("div");
  tooltip.className = "tooltip";
  tooltip.popover = "manual"; // above everything, an open hint too
  tooltip.setAttribute("aria-hidden", "true");
  let tipFor = null; // what the tooltip was last asked about
  let described = null; // the element that it is about
  let held = []; // the titles that it took, [element, title], the nearest first

  const nearest = (el, test) => {
    while (el && !test(el)) el = el.parentElement;
    return el;
  };
  const cuts = (el) => el.scrollWidth > el.clientWidth && getComputedStyle(el).textOverflow === "ellipsis";

  // describe shows the tooltip of target, inside the window: over its
  // element, or under it where there is no room. It measures the tooltip
  // once. With no target, or nothing to say, it hides the tooltip.
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
    tooltip.textContent = title ? (box || !described.innerText?.includes(title) ? title : "") : described?.innerText ?? "";
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
    const x = Math.min(at.left + parseFloat(getComputedStyle(described).paddingLeft) - 9, root.clientWidth - tooltip.offsetWidth - 8);
    const y = at.top - tooltip.offsetHeight - 6;
    tooltip.style.transform = `translate(${Math.round(Math.max(8, x))}px, ${Math.round(y < 8 ? at.bottom + 6 : y)}px)`;
  }

  // The tooltip goes with a press, a key, a scroll, and the focus, and when
  // the pointer leaves the window.
  const undescribe = () => {
    if (tipFor) describe(null);
  };
  for (const type of ["pointerdown", "keydown", "focusout", "scroll"]) document.addEventListener(type, undescribe, true);
  root.addEventListener("pointerleave", undescribe);
  document.addEventListener("pointerover", (event) => {
    if (event.pointerType !== "touch" && !event.buttons) describe(event.target);
  });
  // The focus may scroll the page to its element, which hides the tooltip:
  // it shows after that, in the same frame.
  document.addEventListener("focusin", ({ target }) => {
    requestAnimationFrame(() => {
      if (target.matches?.(":focus-visible")) describe(target);
    });
  });
  // A finger cannot hover: a tap on text that leads nowhere shows the
  // tooltip, until the next tap.
  document.addEventListener("click", ({ target }) => {
    if (coarse.matches && target.closest?.("a, button, summary, label, [data-href]") === null) describe(target);
  });

  // Charts: the values of a step
  //
  // A guide marks the step of a chart that is under the pointer or a pressed
  // finger, or that the chart's slider is on. A note beside it gives the
  // step's time and values. The page holds them all: each series has its
  // values as text, and the legend names them. The guide is an element in
  // the chart, so it scrolls and resizes with it. A live update removes it,
  // as it removes whatever the server did not render, and draws it again
  // from the newer values before the browser paints.

  const guide = document.createElement("div");
  guide.className = "chart-guide";
  guide.id = "chart-guide"; // a name of its own: an update never takes it for a part of the page
  guide.setAttribute("aria-hidden", "true"); // the chart's slider says the step
  const tip = guide.appendChild(document.createElement("div"));
  tip.className = "chart-tip";
  let traced = null; // the chart with the guide, and the pointer's place or the step that its slider is on: { svg, x } or { svg, i }
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
    document.addEventListener(type, (event) => {
      const svg = event.target.closest?.(".chart-plot > svg");
      if (!svg) return;
      traced = { svg, x: event.clientX };
      tracing ||= requestAnimationFrame(trace);
    });
  }
  // The pointer left the chart, or the finger lifted.
  document.addEventListener("pointerout", (event) => {
    if (traced?.svg.contains(event.target) && !traced.svg.contains(event.relatedTarget)) untrace();
  });

  // A chart has a slider for the keyboard and for a screen reader: a range
  // field over the plot, which nobody sees. The browser moves it, from the
  // newest step, and the guide follows it while it has the focus; Escape
  // hides the guide. say gives the slider the words of its step, the
  // tooltip of the step's column: a screen reader reads them as its value.
  function say(slider) {
    const words = slider.previousElementSibling.querySelectorAll(".col")[slider.value - 1]?.textContent ?? "";
    if (slider.getAttribute("aria-valuetext") !== words) slider.setAttribute("aria-valuetext", words);
  }

  for (const type of ["focusin", "input"]) {
    document.addEventListener(type, (event) => {
      const slider = event.target;
      if (!slider.matches?.(".chart-step")) return;
      say(slider);
      traced = { svg: slider.previousElementSibling, i: slider.value - 1 };
      trace();
    });
  }
  document.addEventListener("focusout", (event) => {
    if (traced?.svg.nextElementSibling === event.target) untrace();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && event.target.matches?.(".chart-step")) untrace();
  });

  // Times in the reader's time zone
  //
  // The server words every time in UTC, and gives the time's instant beside
  // the words, in data-time. localise words each again in the time zone of
  // the device: the tooltip of a time and, in the filter by time, the
  // range on the button, its two bounds, and the name of the zone. It
  // changes a page before the page shows, so an update finds the words that
  // it already shows, and touches nothing. The words are the server's own,
  // put together from what the device says of an instant: the device knows
  // when its zone changes its clocks.

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
      el.title = `${dateOf(at)} ${pad(at.getHours())}:${pad(at.getMinutes())}:${pad(at.getSeconds())}.${pad(at.getMilliseconds(), 3)} ${zoneOf(at)}`;
    }
    // The range of the filter by time, as the server words it: in short on
    // the button, and in full, with the years and the zones, in its tooltip
    // and for a screen reader. Its end is the last minute before "before",
    // or the last second, as the fields have it.
    const sel = scope.querySelector(".sel-range");
    if (!sel) return;
    const [since, before] = [...sel.querySelectorAll("input[data-time]")].map((field) => field.dataset.time && new Date(field.dataset.time));
    const whole = before && !before.getSeconds();
    const until = before && new Date(before - (whole ? 60000 : 1000));
    const clock = (at) => `${clockOf(at)}${at === until && !whole && !at.getSeconds() ? ":00" : ""}`;
    const year = new Date().getFullYear();
    const day = (at, always) => `${at.getDate()} ${MONTHS[at.getMonth()]}${always || at.getFullYear() !== year ? ` ${pad(at.getFullYear(), 4)}` : ""}`;
    const brief = (at) => `${day(at)} ${clock(at)}`;
    const long = (at) => `${day(at, true)} ${clock(at)} ${zoneOf(at)}`;
    const words = (from, to) => (since && before ? `${from(since)} to ${to(until)}`
      : since ? `Since ${from(since)}` : before ? `Until ${from(until)}` : "Any time");
    const summary = sel.querySelector("summary");
    const full = words(long, long);
    summary.querySelector(".sel-label").textContent = words(brief, (at) => (dateOf(at) === dateOf(since) ? clock(at) : brief(at)));
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

  // Relative times that keep counting
  //
  // The server words a time by how far it is from the time of rendering:
  // "8s ago". The script counts from there, second by second, in the same
  // words and by the server's clock: the device's may be minutes off. A page
  // brings its time of rendering, in data-now, and the time since it began
  // to arrive is added: by the monotonic clock, or by the device's clock if
  // that counted more, as it does where the other stops in sleep.

  let ahead = NaN; // the server's clock, less the monotonic one
  let aheadOfDevice = NaN; // and less the device's
  let beat = 0; // the timer of the next second

  // arrived takes the server's time from a page that began to arrive at since.
  function arrived(main, since) {
    ahead = Date.parse(main.dataset.now) - since;
    aheadOfDevice = ahead + performance.now() - Date.now();
  }
  const clock = () => Math.max(ahead + performance.now(), aheadOfDevice + Date.now());

  // ago words a number of seconds as ago in ui.go does.
  function ago(s) {
    const d = Math.abs(s);
    const n = d < 60 ? `${d}s` : d < 3600 ? `${Math.trunc(d / 60)}m` : d < 172800 ? `${Math.trunc(d / 3600)}h` : `${Math.trunc(d / 86400)}d`;
    return !d ? "now" : s < 0 ? `in ${n}` : `${n} ago`;
  }

  // span words the seconds of a gap that counts as span in ui.go does.
  function span(s) {
    s = Math.max(0, s);
    const [n, unit, rest, sub] = s < 60 ? [s, "s", 0] : s < 3600 ? [s / 60, "m", s % 60, "s"]
      : s < 172800 ? [s / 3600, "h", (s / 60) % 60, "m"] : [s / 86400, "d", (s / 3600) % 24, "h"];
    return `${Math.trunc(n)}${unit}${Math.trunc(rest) ? ` ${Math.trunc(rest)}${sub}` : ""}`;
  }

  // count words the relative times of a page for the clock's second, before
  // the page shows and while it does. data-now says for which second they
  // are worded. A relative time is the end of the text of an element with a
  // time, if it is that time's words for that second: "paused" stays. A gap
  // that lasts until now, from the time in data-since, counts in the same way.
  function count(main) {
    const from = Math.floor(Date.parse(main.dataset.now) / 1000);
    const to = Math.floor(clock() / 1000);
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
    const now = clock();
    if (document.hidden || Number.isNaN(now)) return;
    if (window.getSelection()?.isCollapsed !== false) count(document.getElementById("main"));
    note();
    beat = setTimeout(tick, 1000 - (now % 1000));
  }

  // The filter by time: a calendar, and two bounds to type
  //
  // To is the last minute of the range: the address takes the start of the
  // next one, as "before". So a range of days ends at 23:59, with all of
  // its last day. A day is its date as text, "2026-11-19". The calendar
  // counts days in UTC: only a bound is read on the reader's clock.

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
    if (date.value === date.defaultValue && time.value === time.defaultValue) return sel.querySelector(`[name=${name}]`).dataset.time;
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
  function draw(root) {
    const sel = document.querySelector(".sel-range[open]");
    if (!sel) { // the menu closed, or left with its page
      cal = null;
      return;
    }
    root ??= sel;
    const grid = root.querySelector(".cal-grid");
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
    const title = root.querySelector(".cal-title");
    if (title.textContent !== monthName.format(first)) title.textContent = monthName.format(first);
    root.querySelector(".cal-nav:last-child").ariaDisabled = month >= now.slice(0, 7);
    const heads = root.querySelector(".cal-week").children;
    grid.querySelectorAll("button").forEach((cell, i) => {
      const day = iso(start + i * DAY);
      const name = dayName.format(start + i * DAY).replace(",", ""); // "Thursday 19 November 2026"
      // A range without an end shows up to today.
      const between = (lo || hi) && (!lo || day > lo) && (hi ? day < hi : day <= now);
      if (i < 7) heads[i].textContent = name.slice(0, 3);
      cell.dataset.day = day;
      cell.textContent = +day.slice(8);
      cell.className = `cal-day${day.startsWith(month) ? "" : " out"}${between ? " in" : ""}${day === lo ? " lo" : ""}${day === hi ? " hi" : ""}`;
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
  document.addEventListener("click", (event) => {
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

  document.addEventListener("pointerover", (event) => {
    if (!cal?.first) return;
    const day = event.target.closest?.(".cal-day[aria-disabled=false]")?.dataset.day ?? null;
    if (day === cal.over) return;
    cal.over = day;
    draw();
  });

  // The arrow keys move by a day and by a week, Home and End to the ends of
  // the week, and Page Up and Page Down by a month, or with Shift by a year.
  document.addEventListener("keydown", (event) => {
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
  // of it, touching only what differs. State that belongs to the reader
  // stays: an open menu, a checkbox or field they changed, and what the
  // script set: attributes named data-js-*, the words of a chart's slider,
  // and the place of a hint, in its style. No copy brings a style attribute:
  // the content security policy would refuse it, so the server sends none,
  // and a page that is kept leaves it behind.
  //
  // A child with a data-key or an id is matched by it, wherever it stands.
  // What stands before the match gives way: a child that the copy lacks,
  // and text between keyed children, are removed. A child that the copy has
  // further on is in the way. If the match is the element right after it,
  // that child is the one out of place: it is set aside, and moves when the
  // copy comes to it. Otherwise the match moves before it. So a row that
  // comes, goes, or moves, up or down, is the one row that is touched, and
  // it keeps the focus. The keys are read once, when a child is first out
  // of place: the work grows with the number of children, not its square.

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
        const keys = new Set(children.slice(at).map(keyOf));
        ahead = new Map();
        aside = new Set();
        for (let n = current; n; n = n.nextSibling) {
          const k = keyOf(n);
          if (k !== null && keys.has(k) && !ahead.has(k)) ahead.set(k, n);
        }
      }
      const match = ahead?.get(key) ?? inPlace;
      if (match && match !== current && !aside.has(match)) {
        while (current !== match) {
          const before = current;
          const stays = ahead.get(keyOf(before)) === before;
          if (stays && match.previousElementSibling !== before) break;
          current = before.nextSibling;
          if (stays) aside.add(before);
          else before.remove();
        }
      }
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
  // "page", "same" (not modified), "down" (no answer from the dashboard: the
  // network, a timeout, or a proxy's error page in its place), "elsewhere"
  // (a page that is not the dashboard's), "signed-out", or "aborted". A
  // page is "failed" when it says that the server failed: it tries again.
  // The page of an action may say why the action did not apply. Its url is
  // where the server led to, if it led somewhere.
  async function request(url, { etag = null, body = null, control = new AbortController() } = {}) {
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; control.abort(); }, TIMEOUT_MS);
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
      arrived(result.main, began);
      return { ...result, failed: response.status >= 500, etag: response.headers.get("ETag"), url: response.redirected ? response.url : null };
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
    // The page changes with its titles on it. Then the tooltip shows again.
    const about = tipFor;
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
      const here = document.querySelector(`[data-live-part="${CSS.escape(part.dataset.livePart)}"]`);
      if (here) morph(here, part);
    }
    if (result.doc.title) document.title = result.doc.title;
    shownETag = result.etag;
    highlight(main);
    for (const slider of main.querySelectorAll(".chart-step")) say(slider);
    if (traced) trace(); // the update took the guide off its chart, or the chart off the page
    if (hint?.matches(":popover-open")) settle();
    // The newer copy lists every option of a menu, and none that a search
    // found on the server: an open menu shows its search again.
    for (const sel of openMenus()) refine(sel);
    if (about?.isConnected) describe(about);
  }

  // Live updates: every few seconds the page fetches itself and updates in
  // place, unless the reader is busy with it. The button in the header shows
  // whether the page is live, paused, out of date, or signed out. Its dot
  // pulses while updates run: not on a page that does not update itself,
  // nor while they wait for the reader. While the server fails, the page on
  // screen stays, and the updates wait longer after each failure. A page
  // that says the server failed is live too: it says when it tries again,
  // and gives way to the page once the server answers.

  const toggle = document.querySelector("[data-live-toggle]");
  const LABELS = { live: "Live", paused: "Paused", stale: "Reconnecting", offline: "Offline", "signed-out": "Signed out" };
  let live = local.get(LIVE_KEY) !== "off";
  let status = "live";
  let failures = 0; // in a row
  let cause = ""; // what the last failure said
  let lastUpdate = Date.now();
  let timer = 0;
  let due = 0; // when the next update runs
  let waiting = false; // the last update waited for the reader

  const livePage = () => document.getElementById("main")?.hasAttribute("data-live") ?? false;
  const retrying = () => document.getElementById("main")?.dataset.live === "retry";

  function show() {
    if (!toggle) return;
    let state = live ? status : "paused";
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
    note();
  }

  // note says, on a page that says the server failed, when it tries again.
  function note() {
    const said = document.querySelector("[data-retry]");
    if (!said) return;
    const s = Math.ceil((due - Date.now()) / 1000);
    said.textContent = !live ? "Live updates are paused" : status === "signed-out" ? ""
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

  function schedule(ms) {
    clearTimeout(timer);
    due = Date.now() + ms;
    timer = setTimeout(update, ms);
  }

  function succeeded(took = 0) {
    failures = 0;
    lastUpdate = Date.now();
    status = "live";
    // A slow server gets more time between updates.
    schedule(Math.max(REFRESH_MS, 2 * took));
    show();
  }

  // failed counts a failure, says what it was, in the title of the page that
  // said it, and waits longer before the next update: 5 seconds after the
  // first, and twice as long after each more, up to a minute.
  const heading = (main) => main.querySelector("h1")?.textContent ?? "";

  function failed(why = "") {
    failures++;
    cause = why;
    status = "stale";
    schedule(Math.min(MAX_WAIT_MS, REFRESH_MS * 2 ** (failures - 1)));
    show();
  }

  // landed starts the updates of a page that the reader asked for. Their
  // request is a fresh start: a page that says the server failed tries
  // again 5 seconds later.
  function landed(result) {
    failures = 0;
    if (result.failed) failed(heading(document.getElementById("main")));
    else succeeded();
  }

  async function update() {
    timer = 0;
    if (!live || status === "signed-out" || document.hidden || !livePage()) return;
    if (waiting !== busy()) {
      waiting = !waiting;
      show();
    }
    // A request in flight is a navigation: it must not be abandoned for an
    // update of the page it replaces.
    if (waiting || inflight) {
      schedule(1000);
      return;
    }
    const started = Date.now();
    note();
    const result = await load(location.href, shownETag);
    if (!live) return; // the reader paused
    if (result.kind === "aborted") { // another page took over: it plans the next update, or this does
      if (!timer) schedule(REFRESH_MS);
      return;
    }
    if (result.kind === "signed-out") {
      status = "signed-out";
      show();
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

  // A tab that comes back in view updates when its update is due: at once
  // if it fell due while the tab was hidden, which runs none.
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) return;
    schedule(Math.max(0, (failures ? due : lastUpdate + REFRESH_MS) - Date.now()));
    tick();
  });
  window.addEventListener("online", () => {
    failures = 0;
    schedule(0);
  });
  window.addEventListener("offline", show);

  highlight(document);
  localise(document);
  for (const slider of document.querySelectorAll(".chart-step")) say(slider);
  arrived(document.getElementById("main"), performance.getEntriesByType("navigation")[0]?.responseStart || performance.now());
  if (retrying()) failed(heading(document.getElementById("main")));
  else if (live) schedule(REFRESH_MS);
  tick();
  show();
})();
