import * as Plot from "@observablehq/plot";
import * as d3 from "d3";

interface CalendarEntry {
  date: string;
  hours: number;
  flights: number;
}

export function renderCalendarChart(
  container: HTMLElement,
  data: CalendarEntry[]
): void {
  container.innerHTML = "";

  const dataMap = new Map(data.map((d) => [d.date, d]));

  const dates = data.map((d) => new Date(d.date + "T00:00:00Z"));
  const minDate = d3.min(dates)!;
  const maxDate = d3.max(dates)!;

  const startDate = d3.utcSunday(minDate);
  const endDate = d3.utcDay.offset(maxDate, 1);
  const allDays = d3.utcDays(startDate, endDate);

  const fullData = allDays.map((dateObj) => {
    const key = d3.utcFormat("%Y-%m-%d")(dateObj);
    const entry = dataMap.get(key);
    const weekIndex = d3.utcSunday.count(startDate, dateObj);
    const dayOfWeek = dateObj.getUTCDay();
    return {
      date: key,
      dateObj,
      hours: entry?.hours ?? 0,
      flights: entry?.flights ?? 0,
      weekIndex,
      dayOfWeek,
    };
  });

  const months: { label: string; weekIndex: number }[] = [];
  let lastYM = "";
  for (const d of fullData) {
    const ym = d3.utcFormat("%Y-%m")(d.dateObj);
    if (ym !== lastYM) {
      months.push({
        label: d3.utcFormat("%b")(d.dateObj),
        weekIndex: d.weekIndex,
      });
      lastYM = ym;
    }
  }

  const maxHours = d3.max(fullData, (d) => d.hours) ?? 10;
  const totalWeeks =
    d3.utcSunday.count(startDate, d3.utcSunday.ceil(endDate)) + 1;

  const chart = Plot.plot({
    width: Math.max(container.clientWidth || 800, totalWeeks * 16 + 60),
    height: 7 * 16 + 60,
    padding: 0,
    marginTop: 28,
    marginLeft: 32,
    marginBottom: 8,
    marginRight: 8,
    style: {
      background: "transparent",
      color: "#e2e2e5",
      fontSize: "11px",
    },
    x: {
      axis: null,
      domain: d3.range(totalWeeks),
    },
    y: {
      axis: "left",
      domain: d3.range(7),
      tickFormat: (d: number) => {
        if (d === 1) return "Mon";
        if (d === 3) return "Wed";
        if (d === 5) return "Fri";
        return "";
      },
      tickSize: 0,
      label: null,
    },
    color: {
      type: "linear",
      domain: [0, maxHours],
      range: ["#1a1c2e", "#f59e0b"],
      label: "Hours",
    },
    marks: [
      Plot.text(months, {
        x: (d: { weekIndex: number }) => d.weekIndex,
        y: -1,
        text: (d: { label: string }) => d.label,
        fill: "#9ca3af",
        fontSize: 10,
        dy: -4,
        textAnchor: "start",
      }),
      Plot.cell(fullData, {
        x: "weekIndex",
        y: "dayOfWeek",
        fill: "hours",
        inset: 1.5,
        rx: 2,
      }),
    ],
  });

  container.appendChild(chart);

  // Tooltip — attach directly to each rect via addEventListener
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
    lineHeight: "1.5",
    whiteSpace: "pre",
  });
  document.body.appendChild(tooltip);

  const cellGroup = (chart as SVGSVGElement).querySelector(
    'g[aria-label*="cell"]'
  );
  if (cellGroup) {
    const rects = cellGroup.querySelectorAll("rect");
    rects.forEach((rect, i) => {
      if (i >= fullData.length) return;
      const entry = fullData[i];

      // Make sure each rect captures pointer events
      (rect as SVGRectElement).style.pointerEvents = "all";
      (rect as SVGRectElement).style.cursor = "default";

      rect.addEventListener("pointerenter", (e: Event) => {
        const pe = e as PointerEvent;
        tooltip.textContent = `${entry.date}\n${entry.hours.toFixed(1)} hours\n${entry.flights} flight${entry.flights !== 1 ? "s" : ""}`;
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
