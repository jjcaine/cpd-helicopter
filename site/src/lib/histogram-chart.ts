import * as Plot from "@observablehq/plot";

interface HistogramEntry {
  hour: number;
  flights: number;
}

function formatHour(hour: number): string {
  if (hour === 0) return "12a";
  if (hour < 12) return `${hour}a`;
  if (hour === 12) return "12p";
  return `${hour - 12}p`;
}

function formatHourLong(hour: number): string {
  if (hour === 0) return "12 AM";
  if (hour < 12) return `${hour} AM`;
  if (hour === 12) return "12 PM";
  return `${hour - 12} PM`;
}

export function renderHistogramChart(
  container: HTMLElement,
  data: HistogramEntry[]
): void {
  container.innerHTML = "";

  const maxFlights = Math.max(...data.map((d) => d.flights));
  const yMax = Math.ceil(maxFlights * 1.12);

  const sleepHoursLate = data.filter((d) => d.hour >= 21);
  const sleepHoursEarly = data.filter((d) => d.hour <= 5);

  const chart = Plot.plot({
    width: container.clientWidth || 800,
    height: 320,
    marginTop: 24,
    marginLeft: 44,
    marginBottom: 36,
    marginRight: 16,
    style: {
      background: "transparent",
      color: "#e2e2e5",
      fontSize: "12px",
    },
    x: {
      label: null,
      tickFormat: (d: number) => (d % 3 === 0 ? formatHour(d) : ""),
      domain: data.map((d) => d.hour),
      padding: 0.15,
    },
    y: {
      label: "Flights",
      grid: true,
      domain: [0, yMax],
      labelOffset: 36,
    },
    marks: [
      Plot.rectY(sleepHoursLate, {
        x: "hour",
        y1: 0,
        y2: yMax,
        fill: "rgba(248, 113, 113, 0.07)",
        insetLeft: -6,
        insetRight: -6,
      }),
      Plot.rectY(sleepHoursEarly, {
        x: "hour",
        y1: 0,
        y2: yMax,
        fill: "rgba(248, 113, 113, 0.07)",
        insetLeft: -6,
        insetRight: -6,
      }),
      Plot.text([{ hour: 2, y: yMax * 0.95 }], {
        x: "hour",
        y: "y",
        text: ["Sleep hours"],
        fill: "rgba(248, 113, 113, 0.5)",
        fontSize: 10,
        fontStyle: "italic",
      }),
      Plot.text([{ hour: 22, y: yMax * 0.95 }], {
        x: "hour",
        y: "y",
        text: ["Sleep hours"],
        fill: "rgba(248, 113, 113, 0.5)",
        fontSize: 10,
        fontStyle: "italic",
      }),
      Plot.barY(data, {
        x: "hour",
        y: "flights",
        fill: "#f59e0b",
        rx: 2,
      }),
      Plot.ruleY([0]),
    ],
  });

  container.appendChild(chart);

  // Tooltip — attach directly to each bar rect
  const tooltip = document.createElement("div");
  Object.assign(tooltip.style, {
    position: "fixed",
    pointerEvents: "none",
    background: "#1a1c1e",
    border: "1px solid #444",
    padding: "6px 10px",
    fontSize: "12px",
    color: "#e2e2e5",
    display: "none",
    zIndex: "9999",
    fontFamily: "Inter, sans-serif",
  });
  document.body.appendChild(tooltip);

  const barGroup = (chart as SVGSVGElement).querySelector(
    'g[aria-label*="bar"]'
  );
  if (barGroup) {
    const rects = barGroup.querySelectorAll("rect");
    rects.forEach((rect, i) => {
      if (i >= data.length) return;
      const entry = data[i];

      (rect as SVGRectElement).style.pointerEvents = "all";
      (rect as SVGRectElement).style.cursor = "default";

      rect.addEventListener("pointerenter", (e: Event) => {
        const pe = e as PointerEvent;
        tooltip.textContent = `${formatHourLong(entry.hour)}: ${entry.flights} flights`;
        tooltip.style.display = "block";
        tooltip.style.left = pe.clientX + 14 + "px";
        tooltip.style.top = pe.clientY - 10 + "px";
      });

      rect.addEventListener("pointermove", (e: Event) => {
        const pe = e as PointerEvent;
        tooltip.style.left = pe.clientX + 14 + "px";
        tooltip.style.top = pe.clientY - 10 + "px";
      });

      rect.addEventListener("pointerleave", () => {
        tooltip.style.display = "none";
      });
    });
  }
}
