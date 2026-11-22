// Actions: a form sends its action, and the page that answers shows in
// place, with its notice. An action that asks first is a link to the page
// that asks, which the dialog shows: its form sends the action from there.
// dialog.js holds the dialog and the action on its way.

import { assets, assetsOf, loadWhole, notify, NOTICE_MS, on, progress, SLOW_MS } from "./core.js";
import { confirming, dialog, post, release, setPending, settle, showDialogPart, submitting } from "./dialog.js";
import { schedule, signOut, startUpdates } from "./live.js";
import { applyFilters, shut } from "./menus.js";
import { address, entry, nextTurn, render, turns } from "./navigation.js";

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

// showActionResult shows the page that answered an action, in place: the
// page that the action leads to, with its notice, or the page that says
// why the action did not apply. It takes the place of the page of the
// action in the history, as a deleted task's list does that of its page:
// the back button leads to the page before.
function showActionResult(result) {
  const url = result.url ? clean(new URL(result.url)) : new URL(location.href);
  if (assetsOf(result.doc) !== assets) {
    loadWhole(url.href);
    return;
  }

  const elsewhere = url.pathname !== location.pathname;
  const n = address(url, "replace");
  if (n == null) return;
  render(result, { moved: elsewhere || !result.url, n, notice: result.doc.querySelector("[data-toast]") });
  startUpdates(result);
}

// send runs the action of a form on the page, and shows the page that the
// server answers with.
async function send(form) {
  const turn = nextTurn();
  setPending(form, true);

  const slow = setTimeout(() => progress(true), SLOW_MS);
  const offline = navigator.onLine === false;
  const result = await post(form);

  clearTimeout(slow);
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
  // The button is unavailable meanwhile, and keeps the focus. The dialog is
  // not marked busy: a screen reader would not read the status line then.
  button.ariaDisabled = "true";
  said.textContent = said.dataset.busy;
  const offline = navigator.onLine === false;
  const result = await post(confirming);
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

// listen listens to the forms of the page, and to the dialog.
export function listen() {
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

    // The dialog's main button sends the form of the page that it shows,
    // and the dialog stays for the answer.
    if (form.method === "dialog" && event.submitter?.value === "confirm" && confirming) {
      event.preventDefault();
      act();
      return;
    }
    if (form.method !== "post") return;
    event.preventDefault();
    if ("jsPending" in form.dataset) return;
    send(form);
  });

  // A page restored by the back button is live again.
  window.addEventListener("pageshow", (event) => {
    if (!event.persisted) return;
    settle();
    progress(false);
    release();
    schedule(0);
  });
}

// Notices after an action. The server names a notice in the address of
// the page that shows it. The address loses that right away, so a reload
// does not show the notice again.
function clean(url) {
  for (const name of ["done", "count", "skipped", "conflicts"]) url.searchParams.delete(name);
  return url;
}

// showNotice shows the notice that came with the page, and takes it out of
// the address.
export function showNotice() {
  const toast = document.querySelector("[data-toast]");
  if (toast) notify(toast);
  const url = clean(new URL(location.href));
  if (url.href !== location.href) history.replaceState({ n: entry }, "", url);
}
