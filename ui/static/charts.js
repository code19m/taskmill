// A guide marks the step of a chart under the pointer or a pressed finger,
// or that the chart's slider is on, and a note beside it gives the step's
// time and values, which the page holds: each series has its values as
// text, and the legend names them. The guide is an element in the chart,
// so it scrolls and resizes with it. An update removes it, as whatever
// the server did not render, and draws it again before the browser paints.

import { on } from "./core.js";
import { clockOf, zoneOf } from "./times.js";

const guide = document.createElement("div");
guide.className = "chart-guide";
guide.id = "chart-guide"; // a name of its own: an update never takes it for a part of the page
guide.setAttribute("aria-hidden", "true"); // the chart's slider says the step
const tip = guide.appendChild(document.createElement("div"));
tip.className = "chart-tip";
export let traced = null; // the chart with the guide, and the pointer's place or its slider's step: { svg, x } or { svg, i }
let marked = 0; // the step that the guide marks
let tracing = 0; // the frame that will draw the guide: pointer moves between two frames draw once

export function untrace() {
  traced = null;
  guide.remove();
}

export function trace() {
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

// A chart has a slider for the keyboard and a screen reader: a range field
// over the plot, which nobody sees. The browser moves it, from the newest
// step, and the guide follows it while it has the focus; Escape hides the
// guide. say gives the slider, as its value, the words of its step: the
// tooltip of the step's column.
export function say(slider) {
  const words = slider.previousElementSibling.querySelectorAll(".col")[slider.value - 1]?.textContent ?? "";
  if (slider.getAttribute("aria-valuetext") !== words) slider.setAttribute("aria-valuetext", words);
}

// listen listens to the pointer over a chart, and to its slider.
export function listen() {
  for (const type of ["pointerdown", "pointermove"]) {
    on(type, (event) => {
      const svg = event.target.closest?.(".chart-plot > svg");
      if (!svg) return;
      traced = { svg, x: event.clientX };
      tracing ||= requestAnimationFrame(trace);
    });
  }
  // The pointer left the chart, or the finger lifted.
  on("pointerout", (event) => {
    if (traced?.svg.contains(event.target) && !traced.svg.contains(event.relatedTarget)) untrace();
  });

  // The guide follows the slider while it has the focus.
  for (const type of ["focusin", "input"]) {
    on(type, (event) => {
      const slider = event.target;
      if (!slider.matches?.(".chart-step")) return;
      say(slider);
      traced = { svg: slider.previousElementSibling, i: slider.value - 1 };
      trace();
    });
  }
  on("focusout", (event) => {
    if (traced?.svg.nextElementSibling === event.target) untrace();
  });
  on("keydown", (event) => {
    if (event.key === "Escape" && event.target.matches?.(".chart-step")) untrace();
  });
}
