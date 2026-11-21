// Hints and tooltips.
//
// A hint opens beside its button, toward the middle of the window, where
// it has room, and scrolls with the page. Its place, in --inset, which a
// phone's style overrides, is set before it shows and after an update. A
// worker's lists are a hint with up to three buttons: it opens beside the
// one that the browser says opened it.

import { coarse, on, root } from "./core.js";

export let hint = null; // the hint that opened last
let beside = null; // the button that opened it, if the browser said

export function placeHint() {
  const at = (beside?.isConnected ? beside : hint.previousElementSibling).getBoundingClientRect();
  const down = at.top < root.clientHeight / 2;
  const forward = at.left < root.clientWidth / 2;
  const [top, bottom] = down ? [`${at.bottom + scrollY + 6}px`, "auto"]
    : ["auto", `${root.clientHeight - at.top - scrollY + 6}px`];
  const [left, right] = forward ? [`${at.left + scrollX}px`, "auto"] : ["auto", `${root.clientWidth - at.right - scrollX}px`];
  hint.style.setProperty("--inset", `${top} ${right} ${bottom} ${left}`);
}

// The browser shows a title only after a wait. The page shows it at once,
// in a tooltip of its own, for what is under the pointer or has the
// keyboard's focus: the nearest title, but not one that only repeats text
// that shows whole, or else the whole text of what an ellipsis cuts short.
// The tooltip holds the titles, which the browser would show too; a screen
// reader hears them as before, in aria-description.

const TIP_GAP = 6; // between the tooltip and its element
const tooltip = document.createElement("div");
tooltip.className = "tooltip";
tooltip.popover = "manual"; // above everything, an open hint too
tooltip.setAttribute("aria-hidden", "true");
export let tipFor = null; // what the tooltip was last asked about
export let described = null; // the element that it is about
let held = []; // the titles that it took, [element, title], the nearest first
let resting = false; // the pointer is on the tooltip, away from its element

const nearest = (el, test) => {
  while (el && !test(el)) el = el.parentElement;
  return el;
};
const cuts = (el) => el.scrollWidth > el.clientWidth && getComputedStyle(el).textOverflow === "ellipsis";

// describe shows the tooltip of target, inside the window: over its
// element, or under it where there is no room. With no target, or nothing
// to say, it hides the tooltip.
export function describe(target) {
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
  let text = title;
  if (!title) text = described?.innerText ?? "";
  else if (!box && described.innerText?.includes(title)) text = "";
  tooltip.textContent = text;
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
  const x = Math.min(at.left + parseFloat(getComputedStyle(described).paddingLeft) - 9,
    root.clientWidth - tooltip.offsetWidth - 8);
  const y = at.top - tooltip.offsetHeight - TIP_GAP;
  tooltip.style.transform = `translate(${Math.round(Math.max(8, x))}px, ${Math.round(y < 8 ? at.bottom + TIP_GAP : y)}px)`;
}

// The tooltip goes with a press, a key, a scroll, and the focus, and when
// the pointer leaves the window.
export const undescribe = () => {
  resting = false;
  if (tipFor) describe(null);
};

// The pointer may cross to the tooltip and rest on it, to read it: over the
// tooltip, or the gap to its element, the tooltip stays. A press goes
// through it to what lies under it, so the page asks where the pointer is,
// and only while it is there does it ask at each move.
const onTip = ({ clientX: x, clientY: y }) => {
  const box = tooltip.isConnected && tooltip.getBoundingClientRect();
  return box && x >= box.left && x <= box.right && y >= box.top - TIP_GAP - 1 && y <= box.bottom + TIP_GAP + 1;
};

// listen listens to what opens and closes a hint, and to the pointer, the
// focus, and the taps that show the tooltip.
export function listen() {
  on("beforetoggle", (event) => {
    const pop = event.target;
    if (event.newState !== "open" || !pop.matches?.(".hint-pop")) return;
    hint = pop;
    beside = event.source;
    placeHint();
  }, true);

  // The browser closes a hint on a click elsewhere, but not every browser
  // takes a tap on plain text for a click. A press on a button of the hint
  // is not elsewhere: the click that follows closes it.
  on("pointerdown", (event) => {
    if (hint?.matches(":popover-open") && !hint.parentElement.contains(event.target) &&
      event.target.closest?.("[popovertarget]")?.popoverTargetElement !== hint) hint.hidePopover();
  }, true);

  for (const type of ["pointerdown", "keydown", "focusout", "scroll"]) on(type, undescribe, true);
  root.addEventListener("pointerleave", undescribe);

  on("pointerover", (event) => {
    if (event.pointerType === "touch" || event.buttons) return;
    resting = onTip(event);
    if (!resting) describe(event.target);
  });
  on("pointermove", (event) => {
    if (resting && !onTip(event)) {
      resting = false;
      describe(event.target);
    }
  });
  // The focus may scroll the page to its element, which hides the tooltip:
  // it shows after that, in the same frame.
  on("focusin", ({ target }) => {
    requestAnimationFrame(() => {
      if (target.matches?.(":focus-visible")) describe(target);
    });
  });
  // A finger cannot hover: a tap on text that leads nowhere shows the
  // tooltip, until the next tap.
  on("click", ({ target }) => {
    if (coarse.matches && target.closest?.("a, button, summary, label, [data-href]") === null) describe(target);
  });
}
