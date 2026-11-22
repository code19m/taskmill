// An action that asks first asks in the dialog, which shows the part of
// the action's page that asks. An action is then pending, so that a second
// click on a slow network cannot send it twice. This module holds the
// dialog and the action on its way; actions.js sends actions and shows
// their answers. The pages and the live updates need only this part, so
// they do not import actions.js, which imports them.

import { request } from "./core.js";

export const dialog = document.getElementById("confirm-dialog");
export let confirming = null; // the form, of the page that the dialog shows, that its main button sends
export let submitting = false; // an action is on its way: see post

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
  confirming = form;
  if (!dialog.open) dialog.showModal();
  if (!form) cancel.focus(); // the main button is gone, and may have had the focus
}

// showDialogPart shows in the dialog the part of a page that names its
// title, if the page has one: what an action would do, or change on many
// tasks, its next step, or why it failed. The part's filters and sentences
// are the text, its form's button the main button, and the page's notice,
// of the last step, the status.
export function showDialogPart(result) {
  const part = result.main.querySelector("[data-dialog-title]");
  if (!part) return false;
  const form = part.querySelector("form");
  const button = form?.querySelector("button");
  openDialog(part.dataset.dialogTitle, part.querySelectorAll("dl, p"), form, button?.childNodes, button?.className,
    result.doc.querySelector("[data-toast]")?.textContent);
  return true;
}

// post sends the action of form, and returns the page that answers it, as
// request does. Until then the action is on its way: the live updates wait,
// and a second press in the dialog sends nothing.
export async function post(form) {
  submitting = true;
  const result = await request(form.action, { body: new URLSearchParams(new FormData(form)) });
  submitting = false;
  return result;
}

// settle forgets an action that was on its way when the back button kept
// the page: the browser restored it without the answer.
export function settle() {
  submitting = false;
}

// setPending marks a form whose action is on its way, and its buttons as
// unavailable, or ends that. A button keeps the focus, which a disabled one
// would give to the page's body; the mark keeps a press from sending the
// action again (see listen in actions.js).
export function setPending(form, pending) {
  form.toggleAttribute("data-js-pending", pending);
  for (const button of form.querySelectorAll("button")) button.ariaDisabled = pending ? "true" : null;
}

// release ends the pending state of every form. A page that was kept while
// an action was on its way comes back with it.
export function release() {
  for (const form of document.querySelectorAll("form[data-js-pending]")) setPending(form, false);
}
