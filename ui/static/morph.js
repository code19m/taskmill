// morph changes the page to match a newer copy of it, touching only what
// differs. State that belongs to the reader stays: an open menu, a checkbox
// or field they changed, and what the script set: attributes named
// data-js-*, the words of a chart's slider, and the place of a hint, in its
// style. No copy brings a style attribute: the content security policy
// would refuse it, so the server sends none, and a page that is kept leaves
// it behind.
//
// A child with a data-key or an id is matched by it, wherever it stands.
// What stands before the match gives way: a child that the copy lacks, and
// text between keyed children, are removed. A child that the copy has
// further on is in the way. If the match is the element right after it,
// that child is the one out of place: it is set aside, and moves when the
// copy comes to it. Otherwise the match moves before it. So a row that
// comes, goes, or moves, up or down, is the one row that is touched, and it
// keeps the focus. The keys are read once, when a child is first out of
// place: the work grows with the number of children, not its square.

const keyOf = (node) => (node.nodeType === 1 ? node.getAttribute("data-key") ?? (node.id || null) : null);
const kept = (node, name) => name.startsWith("data-js-") || name === "style" || name === "aria-valuetext" ||
  (name === "open" && node.localName === "details");

export function syncAttributes(node, next) {
  for (const { name } of [...node.attributes]) {
    if (!next.hasAttribute(name) && !kept(node, name)) node.removeAttribute(name);
  }
  for (const { name, value } of next.attributes) {
    if (node.getAttribute(name) !== value && !kept(node, name)) node.setAttribute(name, value);
  }
}

export function morph(node, next) {
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
      ahead = keyedFrom(current, children.slice(at));
      aside = new Set();
    }
    const match = ahead?.get(key) ?? inPlace;
    if (match && match !== current && !aside.has(match)) current = giveWay(current, match, ahead, aside);
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
    const after = current.nextSibling;
    current.remove();
    current = after;
  }
}

// keyedFrom maps each key of wanted to the first child from current on with it.
function keyedFrom(current, wanted) {
  const keys = new Set(wanted.map(keyOf));
  const found = new Map();
  for (let n = current; n; n = n.nextSibling) {
    const k = keyOf(n);
    if (k !== null && keys.has(k) && !found.has(k)) found.set(k, n);
  }
  return found;
}

// giveWay clears the way to match, as above, and returns the child it then stands before.
function giveWay(current, match, ahead, aside) {
  while (current !== match) {
    const before = current;
    const stays = ahead.get(keyOf(before)) === before;
    if (stays && match.previousElementSibling !== before) break;
    current = before.nextSibling;
    if (stays) aside.add(before);
    else before.remove();
  }
  return current;
}

// A payload shows its JSON in colors, which morph keeps while its text is
// the same.
const token = /("(?:\\.|[^"\\])*")(\s*:)?|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|\b(true|false|null)\b/g;

export function highlight(scope) {
  for (const pre of scope.querySelectorAll("[data-json]:not([data-js-highlighted])")) {
    const text = pre.textContent;
    pre.dataset.jsHighlighted = "";
    if (text.length > 50_000) continue; // plain text is kinder to slow machines
    const out = document.createDocumentFragment();
    let last = 0;
    for (const match of text.matchAll(token)) {
      if (match.index > last) out.append(text.slice(last, match.index));
      const mark = document.createElement("span");
      if (match[1]) {
        mark.className = match[2] ? "j-key" : "j-str";
        mark.textContent = match[1];
        out.append(mark, match[2] || "");
      } else {
        mark.className = match[3] ? "j-num" : "j-lit";
        mark.textContent = match[0];
        out.append(mark);
      }
      last = match.index + match[0].length;
    }
    out.append(text.slice(last));
    pre.replaceChildren(out);
  }
}
