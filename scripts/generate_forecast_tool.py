import json
import os

with open("visualizations/forecast_data.json") as f:
    data = json.load(f)

css = (
    "* { margin: 0; padding: 0; box-sizing: border-box; }\n"
    "body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #f5f5f5; color: #222; }\n"
    "#header { background: white; border-bottom: 1px solid #e0e0e0; padding: 16px 24px; }\n"
    "#header h1 { font-size: 18px; font-weight: 600; margin-bottom: 2px; }\n"
    "#header p { font-size: 13px; color: #888; }\n"
    "#main { max-width: 860px; margin: 24px auto; padding: 0 16px; }\n"
    "#controls { background: white; border-radius: 12px; border: 1px solid #e0e0e0; padding: 20px; margin-bottom: 20px; display: flex; gap: 12px; align-items: flex-end; }\n"
    ".ctrl { display: flex; flex-direction: column; gap: 6px; flex: 1; }\n"
    ".ctrl.narrow { flex: 0 0 120px; }\n"
    ".ctrl label { font-size: 11px; font-weight: 600; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }\n"
    ".ctrl input { font-size: 14px; padding: 8px 12px; border: 1px solid #ddd; border-radius: 8px; background: white; width: 100%; }\n"
    "#search-btn { background: #222; color: white; border: none; border-radius: 8px; padding: 9px 20px; font-size: 14px; font-weight: 500; cursor: pointer; height: 38px; white-space: nowrap; flex-shrink: 0; }\n"
    "#search-btn:hover { background: #444; }\n"
    "#station-wrap { position: relative; }\n"
    "#station-dropdown { position: absolute; top: 100%; left: 0; right: 0; z-index: 9999; background: white; border: 1px solid #ddd; border-radius: 8px; max-height: 240px; overflow-y: auto; display: none; box-shadow: 0 4px 16px rgba(0,0,0,0.12); margin-top: 4px; }\n"
    ".drop-item { padding: 9px 14px; font-size: 13px; cursor: pointer; border-bottom: 0.5px solid #f0f0f0; }\n"
    ".drop-item:hover { background: #f5f5f5; }\n"
    ".drop-item:last-child { border-bottom: none; }\n"
    "#results { display: none; }\n"
    "#station-header { background: white; border-radius: 12px; border: 1px solid #e0e0e0; padding: 20px 24px; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; }\n"
    "#station-header h2 { font-size: 17px; font-weight: 600; margin-bottom: 3px; }\n"
    "#station-header .meta { font-size: 13px; color: #888; }\n"
    "#station-header .time-badge { background: #f0f0f0; border-radius: 20px; padding: 6px 14px; font-size: 13px; font-weight: 500; white-space: nowrap; }\n"
    "#week-grid { display: grid; grid-template-columns: repeat(7, 1fr); gap: 8px; margin-bottom: 16px; }\n"
    ".day-card { background: white; border-radius: 10px; border: 1px solid #e0e0e0; padding: 10px; cursor: pointer; transition: border-color 0.15s; }\n"
    ".day-card:hover { border-color: #aaa; }\n"
    ".day-card.selected { border-color: #222; border-width: 2px; }\n"
    ".day-card .day-name { font-size: 10px; font-weight: 600; color: #888; text-transform: uppercase; margin-bottom: 3px; }\n"
    ".day-card .day-date { font-size: 12px; font-weight: 500; margin-bottom: 8px; }\n"
    ".day-card .busy-bar { height: 4px; border-radius: 2px; margin-bottom: 6px; }\n"
    ".day-card .busy-label { font-size: 11px; font-weight: 500; }\n"
    ".busy-label.low { color: #2e7d32; }\n"
    ".busy-label.moderate { color: #f57f17; }\n"
    ".busy-label.high { color: #c62828; }\n"
    "#detail-card { background: white; border-radius: 12px; border: 1px solid #e0e0e0; padding: 24px; }\n"
    "#detail-title { font-size: 14px; font-weight: 600; margin-bottom: 6px; }\n"
    "#detail-subtitle { font-size: 12px; color: #888; margin-bottom: 20px; }\n"
    "#chart-wrap { position: relative; height: 160px; margin-bottom: 6px; }\n"
    "#chart-canvas { width: 100%; height: 100%; display: block; }\n"
    "#hour-labels { display: flex; justify-content: space-between; font-size: 10px; color: #bbb; padding: 0 2px; margin-bottom: 20px; }\n"
    "#selected-hour-summary { background: #f9f9f9; border-radius: 8px; padding: 14px 16px; }\n"
    "#selected-hour-summary .sh-label { font-size: 13px; color: #666; }\n"
    "#selected-hour-summary .sh-value { font-size: 16px; font-weight: 600; }\n"
    "#selected-hour-summary .sh-suggestion { font-size: 12px; color: #888; margin-top: 2px; }\n"
    "#legend { display: flex; gap: 16px; margin-top: 14px; }\n"
    ".leg { display: flex; align-items: center; gap: 5px; font-size: 11px; color: #888; }\n"
    ".leg-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }\n"
    "#no-results { background: white; border-radius: 12px; border: 1px solid #e0e0e0; padding: 40px; text-align: center; color: #888; display: none; }\n"
    "#generated-note { font-size: 11px; color: #ccc; text-align: center; margin-top: 20px; padding-bottom: 40px; }\n"
)

js = (
    'document.addEventListener("DOMContentLoaded", function() {\n'
    'var stations = Object.keys(DATA.predictions).sort();\n'
    'var selectedStation = null;\n'
    'var selectedDate = null;\n'
    'var selectedHour = null;\n'
    'var dropdown = document.getElementById("station-dropdown");\n'
    'var input = document.getElementById("station-input");\n'
    'input.addEventListener("input", function() {\n'
    '  var q = this.value.toLowerCase().trim();\n'
    '  dropdown.innerHTML = "";\n'
    '  if (q.length < 2) { dropdown.style.display = "none"; return; }\n'
    '  var matches = stations.filter(function(s) { return s.toLowerCase().includes(q); }).slice(0, 20);\n'
    '  if (matches.length === 0) { dropdown.style.display = "none"; return; }\n'
    '  matches.forEach(function(s) {\n'
    '    var item = document.createElement("div");\n'
    '    item.className = "drop-item";\n'
    '    item.textContent = s;\n'
    '    item.addEventListener("click", function() { input.value = s; dropdown.style.display = "none"; });\n'
    '    dropdown.appendChild(item);\n'
    '  });\n'
    '  dropdown.style.display = "block";\n'
    '});\n'
    'document.addEventListener("click", function(e) {\n'
    '  if (!document.getElementById("station-wrap").contains(e.target)) { dropdown.style.display = "none"; }\n'
    '});\n'
    'function getLevel(p) { return p >= 0.6 ? "high" : p >= 0.3 ? "moderate" : "low"; }\n'
    'function getLevelLabel(p) { return p >= 0.6 ? "Busier than usual" : p >= 0.3 ? "Moderately busy" : "Quieter than usual"; }\n'
    'function getLevelColor(p) { return p >= 0.6 ? "#f44336" : p >= 0.3 ? "#ff9800" : "#4caf50"; }\n'
    'function formatHour(h) {\n'
    '  var ampm = h < 12 ? "AM" : "PM";\n'
    '  var h12 = h === 0 ? 12 : h > 12 ? h - 12 : h;\n'
    '  return h12 + ":00 " + ampm;\n'
    '}\n'
    'function getBestTime(probs, targetHour, win) {\n'
    '  var best = targetHour;\n'
    '  var bestProb = probs[targetHour] !== undefined ? probs[targetHour] : 1;\n'
    '  for (var h = Math.max(0, targetHour - win); h <= Math.min(23, targetHour + win); h++) {\n'
    '    if ((probs[h] || 0) < bestProb) { bestProb = probs[h]; best = h; }\n'
    '  }\n'
    '  return { hour: best, prob: bestProb };\n'
    '}\n'
    'function findStation(query) {\n'
    '  var q = query.toLowerCase().trim();\n'
    '  var exact = stations.find(function(s) { return s.toLowerCase() === q; });\n'
    '  if (exact) return exact;\n'
    '  var partial = stations.filter(function(s) { return s.toLowerCase().includes(q); });\n'
    '  return partial.length > 0 ? partial[0] : null;\n'
    '}\n'
    'function dateToStr(d) {\n'
    '  return d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0") + "-" + String(d.getDate()).padStart(2, "0");\n'
    '}\n'
    'var DAY_SHORT = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];\n'
    'var DAY_FULL = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"];\n'
    'var MONTH_SHORT = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];\n'
    'function getNextDays(n) {\n'
    '  var days = [];\n'
    '  var today = new Date();\n'
    '  today.setHours(0,0,0,0);\n'
    '  for (var i = 0; i < n; i++) { var d = new Date(today); d.setDate(today.getDate() + i); days.push(d); }\n'
    '  return days;\n'
    '}\n'
    'function renderWeekGrid(station, hour) {\n'
    '  var days = getNextDays(14);\n'
    '  var preds = DATA.predictions[station];\n'
    '  var grid = document.getElementById("week-grid");\n'
    '  grid.innerHTML = "";\n'
    '  days.forEach(function(d) {\n'
    '    var ds = dateToStr(d);\n'
    '    var dayPreds = preds[ds] || {};\n'
    '    var prob = dayPreds[hour] !== undefined ? dayPreds[hour] : null;\n'
    '    var level = prob !== null ? getLevel(prob) : "na";\n'
    '    var color = prob !== null ? getLevelColor(prob) : "#e0e0e0";\n'
    '    var label = prob !== null ? getLevelLabel(prob) : "No data";\n'
    '    var pct = prob !== null ? Math.round(prob * 100) + "%" : "";\n'
    '    var barWidth = prob !== null ? Math.max(8, Math.round(prob * 100)) : 8;\n'
    '    var card = document.createElement("div");\n'
    '    card.className = "day-card" + (ds === selectedDate ? " selected" : "");\n'
    '    card.dataset.date = ds;\n'
    '    card.innerHTML =\n'
    '      "<div class=\\"day-name\\">" + DAY_SHORT[d.getDay()] + "</div>" +\n'
    '      "<div class=\\"day-date\\">" + MONTH_SHORT[d.getMonth()] + " " + d.getDate() + "</div>" +\n'
    '      "<div class=\\"busy-bar\\" style=\\"background:" + color + ";width:" + barWidth + "%\\"></div>" +\n'
    '      "<div class=\\"busy-label " + level + "\\">" + (pct ? pct + " " : "") + label + "</div>";\n'
    '    card.addEventListener("click", function() {\n'
    '      selectedDate = this.dataset.date;\n'
    '      document.querySelectorAll(".day-card").forEach(function(c) { c.classList.remove("selected"); });\n'
    '      this.classList.add("selected");\n'
    '      renderDetailChart(station, selectedDate, hour);\n'
    '    });\n'
    '    grid.appendChild(card);\n'
    '  });\n'
    '}\n'
    'function renderDetailChart(station, dateStr, travelHour) {\n'
    '  var preds = DATA.predictions[station];\n'
    '  var dayPreds = preds[dateStr] || {};\n'
    '  selectedHour = travelHour;\n'
    '  var d = new Date(dateStr + "T12:00:00");\n'
    '  document.getElementById("detail-title").textContent = DAY_FULL[d.getDay()] + ", " + MONTH_SHORT[d.getMonth()] + " " + d.getDate();\n'
    '  document.getElementById("detail-subtitle").textContent = "Click any bar to see details for that hour";\n'
    '  var canvas = document.getElementById("chart-canvas");\n'
    '  var wrap = document.getElementById("chart-wrap");\n'
    '  canvas.width = wrap.offsetWidth;\n'
    '  canvas.height = wrap.offsetHeight;\n'
    '  var ctx = canvas.getContext("2d");\n'
    '  ctx.clearRect(0, 0, canvas.width, canvas.height);\n'
    '  var w = canvas.width, h = canvas.height;\n'
    '  var barW = Math.floor(w / 24) - 2;\n'
    '  var maxH = h - 24;\n'
    '  for (var hour = 0; hour < 24; hour++) {\n'
    '    var prob = dayPreds[hour] !== undefined ? dayPreds[hour] : 0;\n'
    '    var barH = Math.max(2, Math.round(prob * maxH));\n'
    '    var x = Math.round(hour * (w / 24)) + 1;\n'
    '    var y = h - barH - 4;\n'
    '    if (hour === travelHour) { ctx.fillStyle = "rgba(0,0,0,0.05)"; ctx.fillRect(x - 2, 0, barW + 4, h); }\n'
    '    ctx.fillStyle = hour === selectedHour ? "#222" : getLevelColor(prob);\n'
    '    ctx.fillRect(x, y, barW, barH);\n'
    '    if (prob > 0.2) {\n'
    '      ctx.fillStyle = "white";\n'
    '      ctx.font = "bold 9px sans-serif";\n'
    '      ctx.textAlign = "center";\n'
    '      ctx.fillText(Math.round(prob * 100) + "%", x + barW / 2, y + 11);\n'
    '    }\n'
    '  }\n'
    '  var labels = document.getElementById("hour-labels");\n'
    '  labels.innerHTML = "";\n'
    '  [0,3,6,9,12,15,18,21].forEach(function(hr) {\n'
    '    var span = document.createElement("span");\n'
    '    var ap = hr < 12 ? "AM" : "PM";\n'
    '    var h12 = hr === 0 ? 12 : hr > 12 ? hr - 12 : hr;\n'
    '    span.textContent = h12 + ap;\n'
    '    labels.appendChild(span);\n'
    '  });\n'
    '  updateHourSummary(travelHour, dayPreds);\n'
    '  canvas.onclick = function(e) {\n'
    '    var rect = canvas.getBoundingClientRect();\n'
    '    var clickX = e.clientX - rect.left;\n'
    '    var clickedHour = Math.min(23, Math.floor(clickX / (canvas.width / 24)));\n'
    '    selectedHour = clickedHour;\n'
    '    renderDetailChart(station, dateStr, clickedHour);\n'
    '  };\n'
    '}\n'
    'function updateHourSummary(hour, dayPreds) {\n'
    '  var prob = dayPreds[hour] !== undefined ? dayPreds[hour] : null;\n'
    '  var summary = document.getElementById("selected-hour-summary");\n'
    '  if (prob === null) { summary.style.display = "none"; return; }\n'
    '  summary.style.display = "block";\n'
    '  var best = getBestTime(dayPreds, hour, 2);\n'
    '  document.getElementById("sh-label").textContent = formatHour(hour);\n'
    '  document.getElementById("sh-value").textContent = getLevelLabel(prob) + " (" + Math.round(prob * 100) + "%)";\n'
    '  document.getElementById("sh-value").style.color = getLevelColor(prob);\n'
    '  var suggText = best.hour !== hour\n'
    '    ? "Quieter nearby: " + formatHour(best.hour) + " at " + Math.round(best.prob * 100) + "%"\n'
    '    : "This is already the quietest time in this window";\n'
    '  document.getElementById("sh-suggestion").textContent = suggText;\n'
    '}\n'
    'document.getElementById("search-btn").addEventListener("click", function() {\n'
    '  var query = document.getElementById("station-input").value;\n'
    '  var station = findStation(query);\n'
    '  document.getElementById("no-results").style.display = "none";\n'
    '  document.getElementById("results").style.display = "none";\n'
    '  if (!station) { document.getElementById("no-results").style.display = "block"; return; }\n'
    '  selectedStation = station;\n'
    '  var travelHour = parseInt(document.getElementById("travel-time").value.split(":")[0]);\n'
    '  var meta = DATA.meta[station] || {};\n'
    '  document.getElementById("station-name").textContent = station;\n'
    '  document.getElementById("station-meta").textContent = (meta.borough || "") + (meta.tier ? " - Tier " + meta.tier + " station" : "");\n'
    '  var ampm = travelHour < 12 ? "AM" : "PM";\n'
    '  var h12 = travelHour === 0 ? 12 : travelHour > 12 ? travelHour - 12 : travelHour;\n'
    '  document.getElementById("time-badge").textContent = h12 + ":00 " + ampm;\n'
    '  var today = new Date();\n'
    '  today.setHours(0,0,0,0);\n'
    '  selectedDate = dateToStr(today);\n'
    '  document.getElementById("results").style.display = "block";\n'
    '  renderWeekGrid(station, travelHour);\n'
    '  renderDetailChart(station, selectedDate, travelHour);\n'
    '});\n'
    'document.getElementById("station-input").addEventListener("keydown", function(e) {\n'
    '  if (e.key === "Enter") document.getElementById("search-btn").click();\n'
    '});\n'
    'document.getElementById("generated-note").textContent =\n'
    '  "Forecast generated " + DATA.generated_at + " covering " + DATA.start_date + " to " + DATA.end_date +\n'
    '  ". Busyness is relative to each station historical volume, not absolute crowding.";\n'
    '}); // end DOMContentLoaded\n'
)

html_head = (
    '<!DOCTYPE html>\n<html>\n<head>\n<meta charset="utf-8">\n'
    '<title>NYC Subway Station Busyness Planner</title>\n'
    '<style>\n' + css + '</style>\n</head>\n<body>\n'
    '<div id="header">\n'
    '  <h1>NYC Subway Station Busyness Planner</h1>\n'
    '  <p>How busy will your station be? Based on turnstile entries across 428 stations. Busyness is relative to each station historical volume.</p>\n'
    '</div>\n'
    '<div id="main">\n'
    '  <div id="controls">\n'
    '    <div class="ctrl">\n'
    '      <label>Station</label>\n'
    '      <div id="station-wrap">\n'
    '        <input type="text" id="station-input" placeholder="Search for a station..." autocomplete="off"/>\n'
    '        <div id="station-dropdown"></div>\n'
    '      </div>\n'
    '    </div>\n'
    '    <div class="ctrl narrow">\n'
    '      <label>Time</label>\n'
    '      <input type="time" id="travel-time" value="08:00"/>\n'
    '    </div>\n'
    '    <button id="search-btn">Show forecast</button>\n'
    '  </div>\n'
    '  <div id="no-results">Station not found. Try a partial name like Grand Central or Times Sq.</div>\n'
    '  <div id="results">\n'
    '    <div id="station-header">\n'
    '      <div><h2 id="station-name"></h2><div class="meta" id="station-meta"></div></div>\n'
    '      <div class="time-badge" id="time-badge"></div>\n'
    '    </div>\n'
    '    <div id="week-grid"></div>\n'
    '    <div id="detail-card">\n'
    '      <div id="detail-title"></div>\n'
    '      <div id="detail-subtitle"></div>\n'
    '      <div id="chart-wrap"><canvas id="chart-canvas"></canvas></div>\n'
    '      <div id="hour-labels"></div>\n'
    '      <div id="selected-hour-summary" style="display:none">\n'
    '        <div class="sh-label" id="sh-label"></div>\n'
    '        <div class="sh-value" id="sh-value"></div>\n'
    '        <div class="sh-suggestion" id="sh-suggestion"></div>\n'
    '      </div>\n'
    '      <div id="legend">\n'
    '        <div class="leg"><span class="leg-dot" style="background:#4caf50"></span>Quieter than usual</div>\n'
    '        <div class="leg"><span class="leg-dot" style="background:#ff9800"></span>Moderately busy</div>\n'
    '        <div class="leg"><span class="leg-dot" style="background:#f44336"></span>Busier than usual</div>\n'
    '      </div>\n'
    '    </div>\n'
    '  </div>\n'
    '  <div id="generated-note"></div>\n'
    '</div>\n'
    '<script>\nvar DATA = '
)

html_tail = ';\n' + js + '</script>\n</body>\n</html>\n'

os.makedirs("visualizations", exist_ok=True)
with open("visualizations/forecast.html", "w", encoding="utf-8") as f:
    f.write(html_head)
    json.dump(data, f, ensure_ascii=True, separators=(",", ":"))
    f.write(html_tail)

print("forecast.html generated successfully")