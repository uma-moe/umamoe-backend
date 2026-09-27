const fs = require("fs");
const path = require("path");
const sharp = require(path.join(process.argv[4], "sharp"));

const input = process.argv[2];
const outputDir = process.argv[3];
if (!input || !outputDir || !process.argv[4]) {
  throw new Error("usage: node plot_circle_rank_threshold_history.cjs <csv> <output-dir> <node-modules>");
}

const lines = fs.readFileSync(input, "utf8").trim().split(/\r?\n/);
const header = lines.shift().split(",");
const ix = Object.fromEntries(header.map((name, index) => [name, index]));
const data = lines.map((line) => {
  const v = line.split(",");
  return {
    month: v[ix.month_start],
    day: Number(v[ix.day_of_month]),
    tier: v[ix.tier],
    boundary: Number(v[ix.boundary_rank]),
    total: Number(v[ix.required_fans]),
    outsideTotal: v[ix.outside_required_fans] ? Number(v[ix.outside_required_fans]) : null,
    daily: Number(v[ix.required_fans_per_day]),
    forecast: v[ix.is_forecast] === "true",
  };
}).filter((row) => !row.forecast);

const tiers = ["SS", "S+", "S", "A+", "A", "B+", "B", "C+", "C"];
const currentMonth = data.map((d) => d.month).sort().at(-1);
const currentDay = Math.max(...data.filter((d) => d.month === currentMonth).map((d) => d.day));
const months = [...new Set(data.map((d) => d.month))].sort();
const historyMonths = months.filter((month) => month !== currentMonth);
const recentMonths = historyMonths.slice(-6);
const width = 2400;
const height = 1580;
const colors = { bg: "#f8fafc", text: "#172033", muted: "#64748b", grid: "#dbe3ec", history: "#64748b", current: "#2563eb", bottom: "#7c3aed", forecast: "#e8580c" };

assert(tiers.every((tier) => data.some((d) => d.tier === tier)), "all nine tiers must exist");
assert(currentDay > 1, "current month needs at least two observations");

function assert(ok, message) {
  if (!ok) throw new Error(message);
}

function escapeXml(value) {
  return String(value).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&apos;" })[c]);
}

function fmt(value) {
  const abs = Math.abs(value);
  if (abs >= 1e9) return `${(value / 1e9).toFixed(abs >= 10e9 ? 0 : 1)}B`;
  if (abs >= 1e6) return `${(value / 1e6).toFixed(abs >= 10e6 ? 0 : 1)}M`;
  if (abs >= 1e3) return `${(value / 1e3).toFixed(abs >= 10e3 ? 0 : 1)}K`;
  return Math.round(value).toString();
}

function niceMax(value) {
  if (value <= 0) return 1;
  const power = 10 ** Math.floor(Math.log10(value));
  const scaled = value / power;
  return (scaled <= 1 ? 1 : scaled <= 2 ? 2 : scaled <= 5 ? 5 : 10) * power;
}

function quantile(values, p) {
  if (!values.length) return null;
  const sorted = values.toSorted((a, b) => a - b);
  const position = (sorted.length - 1) * p;
  const lower = Math.floor(position);
  const fraction = position - lower;
  return sorted[lower + 1] === undefined ? sorted[lower] : sorted[lower] + fraction * (sorted[lower + 1] - sorted[lower]);
}

function rollingMedian(points, radius = 3) {
  return points.map((point, index) => ({
    ...point,
    y: quantile(points.slice(Math.max(0, index - radius), index + radius + 1).map((item) => item.y), 0.5),
  }));
}

function byMonth(tier) {
  const map = new Map();
  for (const row of data.filter((d) => d.tier === tier)) {
    if (!map.has(row.month)) map.set(row.month, []);
    map.get(row.month).push(row);
  }
  for (const rows of map.values()) rows.sort((a, b) => a.day - b.day);
  return map;
}

function pointAt(rows, day) {
  return rows?.find((row) => row.day === day);
}

function monthEndPace(tier, month) {
  return month === currentMonth ? forecast(tier).values.at(-1).value : byMonth(tier).get(month).at(-1).daily;
}

function forecast(tier) {
  const grouped = byMonth(tier);
  const current = grouped.get(currentMonth);
  const anchor = pointAt(current, currentDay);
  const values = [];
  for (let day = currentDay + 1; day <= 30; day++) {
    const candidates = recentMonths.flatMap((month) => {
      const rows = grouped.get(month);
      const base = pointAt(rows, currentDay);
      const target = pointAt(rows, day);
      return base?.total > 0 && target ? [anchor.total * (target.total / base.total) / day] : [];
    });
    values.push({ day, low: quantile(candidates, 0.25), value: quantile(candidates, 0.5), high: quantile(candidates, 0.75) });
  }
  assert(values.every((d) => d.value !== null), `forecast unavailable for ${tier}`);
  return { current, values };
}

function svgStart(title, subtitle, legend) {
  return [
    `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">`,
    `<rect width="100%" height="100%" fill="${colors.bg}"/>`,
    `<style>text{font-family:Inter,Segoe UI,Arial,sans-serif;fill:${colors.text}}.muted{fill:${colors.muted}}.label{paint-order:stroke;stroke:${colors.bg};stroke-width:8;stroke-linejoin:round}.grid{stroke:${colors.grid};stroke-width:1}.history{fill:none;stroke:${colors.history};stroke-width:4}.current{fill:none;stroke:${colors.current};stroke-width:6}.forecast{fill:none;stroke:${colors.forecast};stroke-width:6;stroke-dasharray:14 10}</style>`,
    `<text x="100" y="65" font-size="42" font-weight="500">${escapeXml(title)}</text>`,
    `<text x="100" y="105" font-size="22" class="muted">${escapeXml(subtitle)}</text>`,
    legend,
  ];
}

function panelLayout(index) {
  const margin = { top: 180, right: 70, bottom: 100, left: 100 };
  const gapX = 42;
  const gapY = 56;
  const panelW = (width - margin.left - margin.right - gapX * 2) / 3;
  const panelH = (height - margin.top - margin.bottom - gapY * 2) / 3;
  const col = index % 3;
  const row = Math.floor(index / 3);
  const px = margin.left + col * (panelW + gapX);
  const py = margin.top + row * (panelH + gapY);
  return { row, px, py, left: px + 72, top: py + 48, innerW: panelW - 90, innerH: panelH - 96 };
}

function panelHeader(parts, tier, layout) {
  const boundary = data.find((d) => d.tier === tier).boundary.toLocaleString("en-US");
  parts.push(`<text x="${layout.px}" y="${layout.py + 24}" font-size="26" font-weight="500">${escapeXml(tier)}</text>`);
  parts.push(`<text x="${layout.px + 52}" y="${layout.py + 24}" font-size="19" class="muted">rank ${boundary}</text>`);
}

function axes(parts, layout, maxY, xTicks) {
  [0, 0.5, 1].forEach((fraction) => {
    const yy = layout.top + layout.innerH * (1 - fraction);
    parts.push(`<line x1="${layout.left}" y1="${yy}" x2="${layout.left + layout.innerW}" y2="${yy}" class="grid"/>`);
    parts.push(`<text x="${layout.left - 12}" y="${yy + 7}" text-anchor="end" font-size="17" class="muted">${fmt(maxY * fraction)}</text>`);
  });
  for (const tick of xTicks) {
    const xx = tick.x(layout);
    parts.push(`<line x1="${xx}" y1="${layout.top}" x2="${xx}" y2="${layout.top + layout.innerH}" class="grid" opacity="0.6"/>`);
    if (layout.row === 2) parts.push(`<text x="${xx}" y="${layout.top + layout.innerH + 31}" text-anchor="middle" font-size="17" class="muted">${tick.label}</text>`);
  }
}

function pathFor(points, x, y, xKey = "x", yKey = "y") {
  return points.map((point, index) => `${index ? "L" : "M"}${x(point[xKey]).toFixed(1)},${y(point[yKey]).toFixed(1)}`).join(" ");
}

async function renderHistory() {
  const legend = `<line x1="1510" y1="70" x2="1580" y2="70" class="history"/><text x="1600" y="78" font-size="20">11-day median</text><line x1="1900" y1="70" x2="1970" y2="70" class="current"/><text x="1990" y="78" font-size="20">September</text><line x1="2180" y1="70" x2="2250" y2="70" class="forecast"/><text x="2270" y="78" font-size="20">Forecast</text>`;
  const parts = svgStart("Rank breakpoint movement", "Eleven-day rolling median of each exact cutoff · reset zeros skipped; weekly rate is daily ×7", legend);
  const shownMonths = [...historyMonths, currentMonth];
  const monthGap = 0;
  const xMax = (shownMonths.length - 1) * (31 + monthGap) + 29;

  tiers.forEach((tier, index) => {
    const layout = panelLayout(index);
    const grouped = byMonth(tier);
    const { current, values } = forecast(tier);
    const position = (month, day) => shownMonths.indexOf(month) * (31 + monthGap) + day - 1;
    const rawCompleted = historyMonths.flatMap((month) => {
      const rows = grouped.get(month);
      return rows.filter((point) => point.total > 0).map((point) => ({ x: position(month, point.day), y: point.daily }));
    });
    const rawSeptember = current.filter((point) => point.total > 0).map((point) => ({ x: position(currentMonth, point.day), y: point.daily }));
    const smoothed = rollingMedian([...rawCompleted, ...rawSeptember], 5);
    const completed = smoothed.slice(0, rawCompleted.length);
    const septemberActual = smoothed.slice(rawCompleted.length);
    const currentPoints = [completed.at(-1), ...septemberActual];
    const rawAnchor = rawSeptember.at(-1).y;
    const smoothAnchor = septemberActual.at(-1).y;
    const anchorX = position(currentMonth, currentDay);
    const endX = position(currentMonth, 30);
    const forecastPoints = [
      currentPoints.at(-1),
      ...values.map((point) => {
        const pointX = position(currentMonth, point.day);
        const taper = 1 - (pointX - anchorX) / (endX - anchorX);
        return { x: pointX, y: point.value + (smoothAnchor - rawAnchor) * taper };
      }),
    ];
    const projected = forecastPoints.at(-1).y;
    const maxY = niceMax(Math.max(...completed.map((d) => d.y), ...currentPoints.map((d) => d.y), ...forecastPoints.map((d) => d.y)) * 1.04);
    const x = (value) => layout.left + (value / xMax) * layout.innerW;
    const y = (value) => layout.top + layout.innerH - (value / maxY) * layout.innerH;
    const monthTicks = shownMonths.map((month) => ({
      label: month.slice(5, 7) === "01" ? month.slice(0, 4) : new Date(`${month}T00:00:00Z`).toLocaleString("en-US", { month: "short", timeZone: "UTC" }),
      x: (l) => l.left + (position(month, 15) / xMax) * l.innerW,
    }));

    panelHeader(parts, tier, layout);
    axes(parts, layout, maxY, monthTicks);
    parts.push(`<path d="${pathFor(completed, x, y)}" class="history"/>`);
    parts.push(`<path d="${pathFor(currentPoints, x, y)}" class="current"/>`);
    parts.push(`<path d="${pathFor(forecastPoints, x, y)}" class="forecast"/>`);
    const endpoint = forecastPoints.at(-1);
    parts.push(`<circle cx="${x(endpoint.x)}" cy="${y(endpoint.y)}" r="7" fill="${colors.forecast}"/>`);
    parts.push(`<text x="${x(endpoint.x) - 10}" y="${y(projected) - 17}" text-anchor="end" font-size="18" font-weight="500" class="label">${fmt(projected)}/day</text>`);
    parts.push(`<text x="${x(endpoint.x) - 10}" y="${y(projected) + 7}" text-anchor="end" font-size="16" class="muted label">${fmt(projected * 7)}/week</text>`);
  });

  parts.push(`<text x="100" y="1550" font-size="18" class="muted">Centered eleven-day median smooths short tally/reset pits · ongoing day excluded · September projection uses median growth after day ${currentDay} across ${recentMonths[0].slice(0, 7)}–${recentMonths.at(-1).slice(0, 7)}</text></svg>`);
  await sharp(Buffer.from(parts.join(""))).png().toFile(path.join(outputDir, "circle-rank-threshold-history.png"));
}

async function renderForecast() {
  const legend = `<rect x="1510" y="58" width="70" height="22" fill="${colors.history}" opacity="0.18"/><text x="1600" y="78" font-size="20">Historical middle 50%</text><line x1="1930" y1="70" x2="2000" y2="70" class="current"/><text x="2020" y="78" font-size="20">September actual</text><line x1="2200" y1="70" x2="2270" y2="70" class="forecast"/><text x="2290" y="78" font-size="20">Forecast</text>`;
  const parts = svgStart("September pace forecast by rank threshold", `Actual through day ${currentDay} · forecast follows the median recent-month growth pattern`, legend);

  tiers.forEach((tier, index) => {
    const layout = panelLayout(index);
    const grouped = byMonth(tier);
    const { current, values } = forecast(tier);
    const history = [];
    for (let day = 1; day <= 30; day++) {
      const samples = recentMonths.flatMap((month) => {
        const point = pointAt(grouped.get(month), day);
        return point ? [point.daily] : [];
      });
      history.push({ day, low: quantile(samples, 0.25), mid: quantile(samples, 0.5), high: quantile(samples, 0.75) });
    }
    const maxY = niceMax(Math.max(...history.map((d) => d.high), ...current.map((d) => d.daily), ...values.map((d) => d.high)) * 1.04);
    const x = (day) => layout.left + ((day - 1) / 29) * layout.innerW;
    const y = (value) => layout.top + layout.innerH - (value / maxY) * layout.innerH;
    const xTicks = [1, 10, 20, 30].map((day) => ({ label: `day ${day}`, x: (l) => l.left + ((day - 1) / 29) * l.innerW }));
    const historicalBand = [...history.map((d) => `${x(d.day)},${y(d.high)}`), ...history.toReversed().map((d) => `${x(d.day)},${y(d.low)}`)].join(" ");
    const forecastBand = [...values.map((d) => `${x(d.day)},${y(d.high)}`), ...values.toReversed().map((d) => `${x(d.day)},${y(d.low)}`)].join(" ");
    const currentPoints = current.map((d) => ({ x: d.day, y: d.daily }));
    const futurePoints = [{ day: currentDay, value: current.at(-1).daily }, ...values];

    panelHeader(parts, tier, layout);
    axes(parts, layout, maxY, xTicks);
    parts.push(`<polygon points="${historicalBand}" fill="${colors.history}" opacity="0.16"/>`);
    parts.push(`<path d="${pathFor(history, x, y, "day", "mid")}" class="history" opacity="0.8"/>`);
    parts.push(`<polygon points="${forecastBand}" fill="${colors.forecast}" opacity="0.12"/>`);
    parts.push(`<path d="${pathFor(currentPoints, x, y)}" class="current"/>`);
    parts.push(`<path d="${pathFor(futurePoints, x, y, "day", "value")}" class="forecast"/>`);
    const endpoint = values.at(-1);
    parts.push(`<circle cx="${x(endpoint.day)}" cy="${y(endpoint.value)}" r="7" fill="${colors.forecast}"/>`);
    parts.push(`<text x="${x(endpoint.day) - 10}" y="${y(endpoint.value) - 17}" text-anchor="end" font-size="18" font-weight="500" class="label">${fmt(endpoint.value)}/day</text>`);
    parts.push(`<text x="${x(endpoint.day) - 10}" y="${y(endpoint.value) + 7}" text-anchor="end" font-size="16" class="muted label">${fmt(endpoint.value * 7)}/week</text>`);
  });

  parts.push(`<text x="100" y="1550" font-size="18" class="muted">Shaded ranges show uncertainty, not guarantees · Based on user fan arrays; monthly_points is not used</text></svg>`);
  await sharp(Buffer.from(parts.join(""))).png().toFile(path.join(outputDir, "circle-rank-threshold-forecast.png"));
}

async function renderRankCurve() {
  const plot = { left: 210, right: 130, top: 210, bottom: 190 };
  const innerW = width - plot.left - plot.right;
  const innerH = height - plot.top - plot.bottom;
  const august = historyMonths.at(-1);
  const stats = tiers.map((tier) => {
    const grouped = byMonth(tier);
    const history = historyMonths.map((month) => grouped.get(month).at(-1).daily);
    return {
      tier,
      boundary: grouped.get(august)[0].boundary,
      low: quantile(history, 0.25),
      median: quantile(history, 0.5),
      high: quantile(history, 0.75),
      august: grouped.get(august).at(-1).daily,
      september: forecast(tier).values.at(-1).value,
    };
  });
  const minY = 1e6;
  const maxY = 1e9;
  const x = (index) => plot.left + (index / (tiers.length - 1)) * innerW;
  const y = (value) => plot.top + innerH - ((Math.log10(value) - Math.log10(minY)) / (Math.log10(maxY) - Math.log10(minY))) * innerH;
  const band = [...stats.map((d, i) => `${x(i)},${y(d.high)}`), ...stats.toReversed().map((d, reverseIndex) => `${x(stats.length - 1 - reverseIndex)},${y(d.low)}`)].join(" ");
  const parts = svgStart(
    "How the rank ladder changes",
    "Month-end fans/day on a logarithmic scale · weekly pace is daily ×7",
    `<rect x="1350" y="57" width="70" height="22" fill="${colors.history}" opacity="0.18"/><text x="1440" y="78" font-size="20">Historical middle 50%</text><line x1="1810" y1="70" x2="1880" y2="70" class="current"/><text x="1900" y="78" font-size="20">August</text><line x1="2070" y1="70" x2="2140" y2="70" class="forecast"/><text x="2160" y="78" font-size="20">September forecast</text>`,
  );

  [1e6, 1e7, 1e8, 1e9].forEach((tick) => {
    const yy = y(tick);
    parts.push(`<line x1="${plot.left}" y1="${yy}" x2="${plot.left + innerW}" y2="${yy}" class="grid"/>`);
    parts.push(`<text x="${plot.left - 24}" y="${yy + 8}" text-anchor="end" font-size="21" class="muted">${fmt(tick)}</text>`);
  });
  parts.push(`<polygon points="${band}" fill="${colors.history}" opacity="0.16"/>`);
  parts.push(`<path d="${pathFor(stats.map((d, i) => ({ x: i, y: d.median })), x, y)}" class="history"/>`);
  parts.push(`<path d="${pathFor(stats.map((d, i) => ({ x: i, y: d.august })), x, y)}" class="current"/>`);
  parts.push(`<path d="${pathFor(stats.map((d, i) => ({ x: i, y: d.september })), x, y)}" class="forecast"/>`);
  stats.forEach((point, index) => {
    parts.push(`<circle cx="${x(index)}" cy="${y(point.august)}" r="7" fill="${colors.current}"/>`);
    parts.push(`<circle cx="${x(index)}" cy="${y(point.september)}" r="7" fill="${colors.forecast}"/>`);
    parts.push(`<text x="${x(index)}" y="${plot.top + innerH + 48}" text-anchor="middle" font-size="24" font-weight="500">${escapeXml(point.tier)}</text>`);
    parts.push(`<text x="${x(index)}" y="${plot.top + innerH + 78}" text-anchor="middle" font-size="18" class="muted">rank ${point.boundary.toLocaleString("en-US")}</text>`);
  });
  parts.push(`<text x="${plot.left}" y="${height - 40}" font-size="18" class="muted">The logarithmic scale keeps every tier readable while preserving the actual ratios between thresholds</text></svg>`);
  await sharp(Buffer.from(parts.join(""))).png().toFile(path.join(outputDir, "circle-rank-threshold-ladder.png"));
}

function mixHex(from, to, amount) {
  const parse = (hex) => [1, 3, 5].map((offset) => parseInt(hex.slice(offset, offset + 2), 16));
  const a = parse(from);
  const b = parse(to);
  return `#${a.map((value, index) => Math.round(value + (b[index] - value) * amount).toString(16).padStart(2, "0")).join("")}`;
}

async function renderChangeHeatmap() {
  const shownMonths = [...historyMonths, currentMonth];
  const left = 260;
  const top = 250;
  const cellW = (width - left - 90) / shownMonths.length;
  const cellH = 112;
  const parts = svgStart(
    "Month-over-month pace change",
    "Change in month-end fans/day · September uses the forecast",
    `<text x="1650" y="78" font-size="20" class="muted">orange = lower</text><text x="1940" y="78" font-size="20" class="muted">blue = higher</text>`,
  );

  shownMonths.forEach((month, column) => {
    const label = new Date(`${month}T00:00:00Z`).toLocaleString("en-US", { month: "short", timeZone: "UTC" });
    parts.push(`<text x="${left + column * cellW + cellW / 2}" y="${top - 34}" text-anchor="middle" font-size="20" font-weight="500">${label}</text>`);
    if (month === currentMonth) parts.push(`<line x1="${left + column * cellW + 12}" y1="${top - 18}" x2="${left + (column + 1) * cellW - 12}" y2="${top - 18}" stroke="${colors.forecast}" stroke-width="5"/>`);
  });

  tiers.forEach((tier, row) => {
    const grouped = byMonth(tier);
    const values = shownMonths.map((month) => month === currentMonth ? forecast(tier).values.at(-1).value : grouped.get(month).at(-1).daily);
    const boundary = grouped.get(historyMonths.at(-1))[0].boundary;
    const yy = top + row * cellH;
    parts.push(`<text x="${left - 28}" y="${yy + 48}" text-anchor="end" font-size="24" font-weight="500">${escapeXml(tier)}</text>`);
    parts.push(`<text x="${left - 28}" y="${yy + 76}" text-anchor="end" font-size="17" class="muted">rank ${boundary.toLocaleString("en-US")}</text>`);
    values.forEach((value, column) => {
      const xx = left + column * cellW;
      const change = column ? (value / values[column - 1] - 1) * 100 : null;
      const strength = change === null ? 0 : Math.min(1, Math.abs(change) / 100);
      const fill = change === null ? "#e2e8f0" : change >= 0 ? mixHex("#dbeafe", colors.current, strength) : mixHex("#ffedd5", colors.forecast, strength);
      const textFill = strength > 0.56 ? colors.bg : colors.text;
      parts.push(`<rect x="${xx + 5}" y="${yy + 5}" width="${cellW - 10}" height="${cellH - 10}" rx="8" fill="${fill}"/>`);
      parts.push(`<text x="${xx + cellW / 2}" y="${yy + 54}" text-anchor="middle" font-size="23" font-weight="500" style="fill:${textFill}">${change === null ? "—" : `${change >= 0 ? "+" : ""}${Math.round(change)}%`}</text>`);
      parts.push(`<text x="${xx + cellW / 2}" y="${yy + 80}" text-anchor="middle" font-size="16" style="fill:${textFill}">${fmt(value)}/day</text>`);
    });
  });

  parts.push(`<text x="${left}" y="${height - 160}" font-size="18" class="muted">Each cell compares with the month immediately before it; color intensity is capped at ±100%</text></svg>`);
  await sharp(Buffer.from(parts.join(""))).png().toFile(path.join(outputDir, "circle-rank-threshold-change-heatmap.png"));
}

async function renderTopBottomChange() {
  const shownMonths = [...historyMonths, currentMonth];
  const pace = shownMonths.map((month) => {
    const values = tiers.map((tier) => monthEndPace(tier, month));
    const geometricMean = (slice) => Math.exp(slice.reduce((sum, value) => sum + Math.log(value), 0) / slice.length);
    return { top: geometricMean(values.slice(0, 5)), bottom: geometricMean(values.slice(5)) };
  });
  const changes = pace.slice(1).map((value, index) => ({
    month: shownMonths[index + 1],
    top: (value.top / pace[index].top - 1) * 100,
    bottom: (value.bottom / pace[index].bottom - 1) * 100,
  }));
  const minY = Math.floor(Math.min(-25, ...changes.flatMap((d) => [d.top, d.bottom])) / 50) * 50;
  const maxY = Math.ceil(Math.max(25, ...changes.flatMap((d) => [d.top, d.bottom])) / 50) * 50;
  const plot = { left: 190, right: 170, top: 220, bottom: 190 };
  const innerW = width - plot.left - plot.right;
  const innerH = height - plot.top - plot.bottom;
  const x = (index) => plot.left + (index / (changes.length - 1)) * innerW;
  const y = (value) => plot.top + innerH - ((value - minY) / (maxY - minY)) * innerH;
  const parts = svgStart(
    "Top vs bottom rank momentum",
    "Month-over-month change in fans/day · split at the median breakpoint: A / rank 1,000",
    `<line x1="1610" y1="70" x2="1680" y2="70" stroke="${colors.current}" stroke-width="6"/><text x="1700" y="78" font-size="20">Top: ranks 10–1,000</text><line x1="2040" y1="70" x2="2110" y2="70" stroke="${colors.bottom}" stroke-width="6"/><text x="2130" y="78" font-size="20">Bottom: 3,000–10,000</text>`,
  );

  for (let tick = minY; tick <= maxY; tick += 50) {
    const yy = y(tick);
    parts.push(`<line x1="${plot.left}" y1="${yy}" x2="${plot.left + innerW}" y2="${yy}" class="grid"${tick === 0 ? ` style="stroke:${colors.muted};stroke-width:2"` : ""}/>`);
    parts.push(`<text x="${plot.left - 24}" y="${yy + 8}" text-anchor="end" font-size="21" class="muted">${tick > 0 ? "+" : ""}${tick}%</text>`);
  }
  changes.forEach((point, index) => {
    const label = new Date(`${point.month}T00:00:00Z`).toLocaleString("en-US", { month: "short", timeZone: "UTC" });
    parts.push(`<text x="${x(index)}" y="${plot.top + innerH + 50}" text-anchor="middle" font-size="21" class="muted">${point.month.slice(5, 7) === "01" ? point.month.slice(0, 4) : label}</text>`);
  });
  const historical = changes.slice(0, -1);
  const forecastSegment = changes.slice(-2);
  parts.push(`<path d="${pathFor(historical.map((d, x) => ({ x, y: d.top })), x, y)}" fill="none" stroke="${colors.current}" stroke-width="6"/>`);
  parts.push(`<path d="${pathFor(historical.map((d, x) => ({ x, y: d.bottom })), x, y)}" fill="none" stroke="${colors.bottom}" stroke-width="6"/>`);
  parts.push(`<path d="${pathFor(forecastSegment.map((d, offset) => ({ x: changes.length - 2 + offset, y: d.top })), x, y)}" fill="none" stroke="${colors.current}" stroke-width="6" stroke-dasharray="14 10"/>`);
  parts.push(`<path d="${pathFor(forecastSegment.map((d, offset) => ({ x: changes.length - 2 + offset, y: d.bottom })), x, y)}" fill="none" stroke="${colors.bottom}" stroke-width="6" stroke-dasharray="14 10"/>`);
  historical.forEach((point, index) => {
    parts.push(`<circle cx="${x(index)}" cy="${y(point.top)}" r="7" fill="${colors.current}"/><circle cx="${x(index)}" cy="${y(point.bottom)}" r="7" fill="${colors.bottom}"/>`);
  });
  const last = changes.at(-1);
  const lastX = x(changes.length - 1);
  parts.push(`<circle cx="${lastX}" cy="${y(last.top)}" r="8" fill="${colors.current}"/><circle cx="${lastX}" cy="${y(last.bottom)}" r="8" fill="${colors.bottom}"/>`);
  parts.push(`<text x="${lastX - 18}" y="${y(last.top) - 18}" text-anchor="end" font-size="22" font-weight="500" class="label">Top ${last.top >= 0 ? "+" : ""}${Math.round(last.top)}%</text>`);
  parts.push(`<text x="${lastX - 18}" y="${y(last.bottom) + 32}" text-anchor="end" font-size="22" font-weight="500" class="label">Bottom ${last.bottom >= 0 ? "+" : ""}${Math.round(last.bottom)}%</text>`);
  parts.push(`<text x="${plot.left}" y="${height - 40}" font-size="18" class="muted">Dashed final segment is September’s forecast change versus August</text></svg>`);
  await sharp(Buffer.from(parts.join(""))).png().toFile(path.join(outputDir, "circle-rank-threshold-top-bottom.png"));
}

async function renderBreakpointHistory() {
  const shownMonths = [...historyMonths, currentMonth];
  const points = shownMonths.map((month, index) => {
    const tierIndex = breakpointForMonth(month);
    return { x: index, month, tierIndex, rank: data.find((d) => d.tier === tiers[tierIndex]).boundary };
  });
  const medianRank = quantile(points.slice(0, -1).map((d) => d.rank), 0.5);
  const plot = { left: 240, right: 170, top: 220, bottom: 190 };
  const innerW = width - plot.left - plot.right;
  const innerH = height - plot.top - plot.bottom;
  const x = (index) => plot.left + (index / (points.length - 1)) * innerW;
  const y = (rank) => plot.top + ((Math.log10(rank) - 2) / (4 - 2)) * innerH;
  const parts = svgStart(
    "Where the rank curve bends",
    "Best two-segment fit on log(rank) versus log(fans/day) · lower rank means the elbow is nearer the top",
    `<line x1="1770" y1="70" x2="1840" y2="70" class="current"/><text x="1860" y="78" font-size="20">Detected breakpoint</text><line x1="2180" y1="70" x2="2250" y2="70" class="forecast"/><text x="2270" y="78" font-size="20">September forecast</text>`,
  );
  const rankTicks = [100, 500, 1000, 3000, 10000];
  rankTicks.forEach((rank) => {
    const yy = y(rank);
    const tier = tiers.find((tier) => data.find((d) => d.tier === tier).boundary === rank);
    parts.push(`<line x1="${plot.left}" y1="${yy}" x2="${plot.left + innerW}" y2="${yy}" class="grid"/>`);
    parts.push(`<text x="${plot.left - 24}" y="${yy + 7}" text-anchor="end" font-size="20" class="muted">${tier ?? ""} · ${rank.toLocaleString("en-US")}</text>`);
  });
  const medianY = y(medianRank);
  parts.push(`<line x1="${plot.left}" y1="${medianY}" x2="${plot.left + innerW}" y2="${medianY}" stroke="${colors.forecast}" stroke-width="3" stroke-dasharray="8 9" opacity="0.75"/>`);
  parts.push(`<text x="${plot.left + innerW - 8}" y="${medianY - 14}" text-anchor="end" font-size="19" class="label">historical median: rank ${Math.round(medianRank).toLocaleString("en-US")}</text>`);
  parts.push(`<path d="${pathFor(points.slice(0, -1).map((d) => ({ x: d.x, y: d.rank })), x, y)}" class="current"/>`);
  parts.push(`<path d="${pathFor(points.slice(-2).map((d) => ({ x: d.x, y: d.rank })), x, y)}" class="forecast"/>`);
  points.forEach((point) => {
    const color = point.month === currentMonth ? colors.forecast : colors.current;
    const monthLabel = new Date(`${point.month}T00:00:00Z`).toLocaleString("en-US", { month: "short", timeZone: "UTC" });
    parts.push(`<circle cx="${x(point.x)}" cy="${y(point.rank)}" r="8" fill="${color}"/>`);
    parts.push(`<text x="${x(point.x)}" y="${plot.top + innerH + 48}" text-anchor="middle" font-size="21" class="muted">${point.month.slice(5, 7) === "01" ? point.month.slice(0, 4) : monthLabel}</text>`);
  });
  const latest = points.at(-1);
  parts.push(`<text x="${x(latest.x) - 14}" y="${y(latest.rank) - 18}" text-anchor="end" font-size="22" font-weight="500" class="label">${tiers[latest.tierIndex]} · rank ${latest.rank.toLocaleString("en-US")}</text>`);
  parts.push(`<text x="${plot.left}" y="${height - 40}" font-size="18" class="muted">The breakpoint is descriptive: it marks the strongest change in slope among the nine measured thresholds</text></svg>`);
  await sharp(Buffer.from(parts.join(""))).png().toFile(path.join(outputDir, "circle-rank-threshold-breakpoint.png"));
}

function boundaryRows(tier) {
  return data
    .filter((row) => row.tier === tier && row.total > 0 && row.outsideTotal > 0)
    .toSorted((a, b) => a.month.localeCompare(b.month) || a.day - b.day);
}

function boundaryTimeline() {
  const shownMonths = [...historyMonths, currentMonth];
  const position = (month, day) => shownMonths.indexOf(month) * 31 + day - 1;
  return { shownMonths, position, max: (shownMonths.length - 1) * 31 + 29 };
}

function boundaryHeader(parts, tier, layout, boundary) {
  parts.push(`<text x="${layout.px}" y="${layout.py + 24}" font-size="26" font-weight="500">${escapeXml(tier)}</text>`);
  parts.push(`<text x="${layout.px + 52}" y="${layout.py + 24}" font-size="19" class="muted">rank ${boundary.toLocaleString("en-US")} ↔ ${(boundary + 1).toLocaleString("en-US")}</text>`);
}

async function renderBoundaryPairs() {
  const timeline = boundaryTimeline();
  const parts = svgStart(
    "Inside versus outside each rank cutoff",
    `Seven-day median through completed day ${currentDay} · rank N holds the tier; rank N+1 falls below it`,
    `<line x1="1540" y1="70" x2="1610" y2="70" stroke="${colors.current}" stroke-width="6"/><text x="1630" y="78" font-size="20">Inside: rank N</text><line x1="1970" y1="70" x2="2040" y2="70" stroke="${colors.bottom}" stroke-width="5" stroke-dasharray="12 8"/><text x="2060" y="78" font-size="20">Outside: rank N+1</text>`,
  );

  tiers.forEach((tier, index) => {
    const layout = panelLayout(index);
    const rows = boundaryRows(tier);
    const inside = rollingMedian(rows.map((row) => ({ x: timeline.position(row.month, row.day), y: row.total / row.day })));
    const outside = rollingMedian(rows.map((row) => ({ x: timeline.position(row.month, row.day), y: row.outsideTotal / row.day })));
    const maxY = niceMax(Math.max(...inside.map((d) => d.y)) * 1.04);
    const x = (value) => layout.left + (value / timeline.max) * layout.innerW;
    const y = (value) => layout.top + layout.innerH - (value / maxY) * layout.innerH;
    const monthTicks = timeline.shownMonths.map((month) => ({
      label: month.slice(5, 7) === "01" ? month.slice(0, 4) : new Date(`${month}T00:00:00Z`).toLocaleString("en-US", { month: "short", timeZone: "UTC" }),
      x: (l) => l.left + (timeline.position(month, 15) / timeline.max) * l.innerW,
    }));

    boundaryHeader(parts, tier, layout, rows[0].boundary);
    axes(parts, layout, maxY, monthTicks);
    parts.push(`<path d="${pathFor(inside, x, y)}" fill="none" stroke="${colors.current}" stroke-width="6"/>`);
    parts.push(`<path d="${pathFor(outside, x, y)}" fill="none" stroke="${colors.bottom}" stroke-width="4" stroke-dasharray="10 7"/>`);
    const top = inside.at(-1);
    const bottom = outside.at(-1);
    parts.push(`<circle cx="${x(top.x)}" cy="${y(top.y)}" r="6" fill="${colors.current}"/><circle cx="${x(bottom.x)}" cy="${y(bottom.y)}" r="5" fill="${colors.bottom}"/>`);
    parts.push(`<text x="${x(top.x) - 10}" y="${y(top.y) - 16}" text-anchor="end" font-size="17" font-weight="500" class="label">N: ${fmt(top.y)}/day</text>`);
    parts.push(`<text x="${x(bottom.x) - 10}" y="${y(bottom.y) + 25}" text-anchor="end" font-size="16" class="muted label">N+1: ${fmt(bottom.y)}/day</text>`);
  });

  parts.push(`<text x="100" y="1550" font-size="18" class="muted">Example: S compares rank 100 with rank 101 · Ongoing-day values are excluded</text></svg>`);
  await sharp(Buffer.from(parts.join(""))).png().toFile(path.join(outputDir, "circle-rank-boundary-pairs.png"));
}

function fmtPercent(value) {
  const decimals = value < 0.01 ? 3 : value < 1 ? 2 : 1;
  return `${value.toFixed(decimals)}%`;
}

async function renderBoundaryMargins() {
  const timeline = boundaryTimeline();
  const parts = svgStart(
    "Margin required to cross each rank cutoff",
    `How much more rank N has than rank N+1 · seven-day median through completed day ${currentDay}`,
    `<line x1="1880" y1="70" x2="1950" y2="70" stroke="${colors.current}" stroke-width="6"/><text x="1970" y="78" font-size="20">Breakpoint margin (%)</text>`,
  );

  tiers.forEach((tier, index) => {
    const layout = panelLayout(index);
    const rows = boundaryRows(tier);
    const margin = rollingMedian(rows.map((row) => ({
      x: timeline.position(row.month, row.day),
      y: (row.total / row.outsideTotal - 1) * 100,
    })));
    const maxY = niceMax(Math.max(...margin.map((d) => d.y)) * 1.04);
    const x = (value) => layout.left + (value / timeline.max) * layout.innerW;
    const y = (value) => layout.top + layout.innerH - (value / maxY) * layout.innerH;
    const monthTicks = timeline.shownMonths.map((month) => ({
      label: month.slice(5, 7) === "01" ? month.slice(0, 4) : new Date(`${month}T00:00:00Z`).toLocaleString("en-US", { month: "short", timeZone: "UTC" }),
      x: (l) => l.left + (timeline.position(month, 15) / timeline.max) * l.innerW,
    }));

    boundaryHeader(parts, tier, layout, rows[0].boundary);
    [0, 0.5, 1].forEach((fraction) => {
      const yy = layout.top + layout.innerH * (1 - fraction);
      parts.push(`<line x1="${layout.left}" y1="${yy}" x2="${layout.left + layout.innerW}" y2="${yy}" class="grid"/>`);
      parts.push(`<text x="${layout.left - 12}" y="${yy + 7}" text-anchor="end" font-size="17" class="muted">${fmtPercent(maxY * fraction)}</text>`);
    });
    for (const tick of monthTicks) {
      const xx = tick.x(layout);
      parts.push(`<line x1="${xx}" y1="${layout.top}" x2="${xx}" y2="${layout.top + layout.innerH}" class="grid" opacity="0.6"/>`);
      if (layout.row === 2) parts.push(`<text x="${xx}" y="${layout.top + layout.innerH + 31}" text-anchor="middle" font-size="17" class="muted">${tick.label}</text>`);
    }
    const area = [`${x(margin[0].x)},${y(0)}`, ...margin.map((d) => `${x(d.x)},${y(d.y)}`), `${x(margin.at(-1).x)},${y(0)}`].join(" ");
    parts.push(`<polygon points="${area}" fill="${colors.current}" opacity="0.10"/>`);
    parts.push(`<path d="${pathFor(margin, x, y)}" fill="none" stroke="${colors.current}" stroke-width="5"/>`);
    const latest = margin.at(-1);
    parts.push(`<circle cx="${x(latest.x)}" cy="${y(latest.y)}" r="7" fill="${colors.forecast}"/>`);
    parts.push(`<text x="${x(latest.x) - 10}" y="${y(latest.y) - 16}" text-anchor="end" font-size="18" font-weight="500" class="label">${fmtPercent(latest.y)}</text>`);
  });

  parts.push(`<text x="100" y="1550" font-size="18" class="muted">Margin = (rank N fans ÷ rank N+1 fans − 1) × 100 · Ongoing-day values are excluded</text></svg>`);
  await sharp(Buffer.from(parts.join(""))).png().toFile(path.join(outputDir, "circle-rank-boundary-margins.png"));
}

fs.mkdirSync(outputDir, { recursive: true });
Promise.all([renderHistory(), renderForecast(), renderRankCurve(), renderChangeHeatmap()]).then(() => console.log("wrote chart PNGs"));
