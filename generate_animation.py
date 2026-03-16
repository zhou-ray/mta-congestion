import json
import os

with open('visualizations/animation_data.json') as f:
    data = json.load(f)

html = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>NYC Subway Congestion Animation</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, sans-serif; background: #f5f5f5; display: flex; flex-direction: column; height: 100vh; }
#header { background: white; border-bottom: 1px solid #e0e0e0; padding: 10px 16px; display: flex; flex-wrap: wrap; gap: 10px; align-items: center; }
#header h1 { font-size: 14px; font-weight: 600; color: #222; margin-right: 4px; }
.ctrl-group { display: flex; align-items: center; gap: 6px; }
.ctrl-label { font-size: 12px; color: #666; white-space: nowrap; }
select, button { font-size: 12px; padding: 4px 8px; border: 1px solid #ccc; border-radius: 6px; background: white; cursor: pointer; }
button:hover { background: #f0f0f0; }
button.active { background: #222; color: white; border-color: #222; }
#time-display { font-size: 12px; font-weight: 600; color: #222; min-width: 200px; }
#map { flex: 1; }
#legend { position: absolute; bottom: 56px; left: 16px; z-index: 1000; background: white; padding: 10px 14px; border-radius: 8px; border: 1px solid #ddd; font-size: 12px; }
.leg-row { display: flex; align-items: center; gap: 6px; margin: 3px 0; }
.dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
#bottom-bar { background: white; border-top: 1px solid #e0e0e0; padding: 8px 16px; display: flex; align-items: center; gap: 10px; }
#scrubber { flex: 1; }
#frame-label { font-size: 11px; color: #888; min-width: 36px; text-align: right; }
</style>
</head>
<body>

<div id="header">
  <h1>NYC Subway Congestion</h1>
  <div class="ctrl-group">
    <span class="ctrl-label">Mode</span>
    <select id="mode-select">
      <option value="daily">24h weekday average</option>
      <option value="week">Week of Oct 6-12 2025</option>
    </select>
  </div>
  <div class="ctrl-group">
    <span class="ctrl-label">Show</span>
    <select id="show-select">
      <option value="ridership">Raw ridership</option>
      <option value="congestion">Congestion level</option>
    </select>
  </div>
  <div class="ctrl-group">
    <span class="ctrl-label">Borough</span>
    <select id="borough-filter">
      <option value="all">All</option>
      <option value="Manhattan">Manhattan</option>
      <option value="Brooklyn">Brooklyn</option>
      <option value="Queens">Queens</option>
      <option value="Bronx">Bronx</option>
      <option value="Staten Island">Staten Island</option>
    </select>
  </div>
  <div class="ctrl-group">
    <span class="ctrl-label">Tier</span>
    <select id="tier-filter">
      <option value="all">All</option>
      <option value="4">Tier 4</option>
      <option value="3">Tier 3</option>
      <option value="2">Tier 2</option>
      <option value="1">Tier 1</option>
    </select>
  </div>
  <div class="ctrl-group">
    <span class="ctrl-label">Lines</span>
    <select id="lines-filter">
      <option value="all">Any</option>
      <option value="1">1 line</option>
      <option value="2">2 lines</option>
      <option value="3+">3+</option>
    </select>
  </div>
  <div class="ctrl-group">
    <span class="ctrl-label">Train line</span>
    <select id="line-filter">
      <option value="all">All lines</option>
    </select>
  </div>
  <div class="ctrl-group">
    <button id="play-btn">Play</button>
    <button id="prev-btn">&#8592;</button>
    <button id="next-btn">&#8594;</button>
  </div>
  <div class="ctrl-group">
    <span class="ctrl-label">Speed</span>
    <input type="range" id="speed-slider" min="1" max="10" value="5" step="1" style="width:70px"/>
  </div>
  <div id="time-display">--</div>
</div>

<div id="map"></div>

<div id="bottom-bar">
  <span style="font-size:11px;color:#888">Scrub</span>
  <input type="range" id="scrubber" min="0" max="23" value="0" step="1"/>
  <span id="frame-label">1 / 24</span>
</div>

<div id="legend">
  <div style="font-weight:600;margin-bottom:5px;font-size:11px" id="leg-title">Ridership level</div>
  <div class="leg-row"><span class="dot" style="background:#1a9641"></span>Very low</div>
  <div class="leg-row"><span class="dot" style="background:#a6d96a"></span>Low</div>
  <div class="leg-row"><span class="dot" style="background:#ffffbf"></span>Moderate</div>
  <div class="leg-row"><span class="dot" style="background:#fdae61"></span>High</div>
  <div class="leg-row"><span class="dot" style="background:#E24B4A"></span>Peak</div>
</div>

<script>
var DATA = """ + json.dumps(data) + """;

var DAY_NAMES = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
var weekTimestamps = Object.keys(DATA.week).sort();
var mode = 'daily';
var showMode = 'ridership';
var currentFrame = 0;
var playing = false;
var timer = null;
var interpTimer = null;

var map = L.map('map').setView([40.730, -73.935], 11);
L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
  attribution: '&copy; OpenStreetMap &copy; CARTO',
  maxZoom: 18
}).addTo(map);

var markers = {};
var currentValues = {};

function lerp(a, b, t) { return a + (b-a)*t; }

function getColor(n) {
  var stops = [
    [0.0,  [26,  150, 65]],
    [0.25, [166, 217, 106]],
    [0.5,  [255, 255, 191]],
    [0.75, [253, 174, 97]],
    [1.0,  [226, 75,  74]]
  ];
  n = Math.min(1, Math.max(0, n));
  for (var i = 0; i < stops.length - 1; i++) {
    var t0 = stops[i][0], t1 = stops[i+1][0];
    if (n <= t1) {
      var t = (n - t0) / (t1 - t0);
      var c0 = stops[i][1], c1 = stops[i+1][1];
      return 'rgb(' +
        Math.round(lerp(c0[0],c1[0],t)) + ',' +
        Math.round(lerp(c0[1],c1[1],t)) + ',' +
        Math.round(lerp(c0[2],c1[2],t)) + ')';
    }
  }
  return 'rgb(226,75,74)';
}

function getCongestionColor(n) {
  if (n > 0.7) return '#E24B4A';
  if (n > 0.4) return '#fdae61';
  if (n > 0.2) return '#ffffbf';
  return '#a6d96a';
}

// After DATA is defined, populate line filter
var lineSelect = document.getElementById('line-filter');
if (DATA.all_lines) {
  DATA.all_lines.forEach(function(l) {
    var opt = document.createElement('option');
    opt.value = l;
    opt.textContent = l + ' train';
    lineSelect.appendChild(opt);
  });
}

function passesFilter(s) {
  var meta = DATA.meta && DATA.meta[s];
  if (!meta) return true;
  var b = document.getElementById('borough-filter').value;
  var t = document.getElementById('tier-filter').value;
  var l = document.getElementById('lines-filter').value;
  var line = document.getElementById('line-filter').value;
  if (b !== 'all' && meta.borough !== b) return false;
  if (t !== 'all' && meta.station_tier !== parseInt(t)) return false;
  if (l === '1' && meta.num_lines !== 1) return false;
  if (l === '2' && meta.num_lines !== 2) return false;
  if (l === '3+' && meta.num_lines < 3) return false;
  if (line !== 'all' && (!meta.lines || meta.lines.indexOf(line) === -1)) return false;
  return true;
}

function initMarkers() {
  Object.keys(markers).forEach(function(k) { map.removeLayer(markers[k]); });
  markers = {};
  currentValues = {};
  Object.keys(DATA.daily).forEach(function(s) {
    var d = DATA.daily[s];
    var m = L.circleMarker([d.lat, d.lon], {
      radius: 4,
      fillColor: '#1a9641',
      color: 'rgba(255,255,255,0.6)',
      weight: 0.8,
      fillOpacity: 0.85
    }).addTo(map);
    m.bindPopup('');
    markers[s] = m;
    currentValues[s] = 0;
  });
}

function getFrameData(frame) {
  if (mode === 'daily') {
    var result = {};
    Object.keys(DATA.daily).forEach(function(s) {
      var h = DATA.daily[s].hours[String(frame)];
      if (h) result[s] = h;
    });
    return result;
  } else {
    return DATA.week[weekTimestamps[frame]] || {};
  }
}

var INTERP_STEPS = 20;
var INTERP_MS = 60;

function animateToFrame(frame) {
  var frameData = getFrameData(frame);
  var targets = {};
  Object.keys(markers).forEach(function(s) {
    var d = frameData[s];
    targets[s] = d ? d.n : 0;
  });

  if (interpTimer) clearInterval(interpTimer);
  var step = 0;
  var startValues = Object.assign({}, currentValues);

  interpTimer = setInterval(function() {
    step++;
    var t = step / INTERP_STEPS;
    t = t < 0.5 ? 2*t*t : -1+(4-2*t)*t;

    Object.keys(markers).forEach(function(s) {
      if (!passesFilter(s)) {
        markers[s].setStyle({ fillOpacity: 0, opacity: 0 });
        return;
      }
      var from = startValues[s] || 0;
      var to = targets[s] || 0;
      var n = lerp(from, to, t);
      currentValues[s] = n;
      var color = showMode === 'ridership' ? getColor(n) : getCongestionColor(n);
      var radius = 3 + n * 9;
      markers[s].setStyle({
        fillColor: color,
        radius: radius,
        fillOpacity: 0.85,
        opacity: 1
      });
      var d = frameData[s];
      if (d) {
        markers[s].setPopupContent(
          '<b style="font-size:12px">' + s + '</b>' +
          '<br><span style="font-size:11px;color:#888">Ridership: </span>' +
          '<span style="font-size:11px">' + Math.round(d.r).toLocaleString() + '</span>'
        );
      }
    });

    if (step >= INTERP_STEPS) {
      clearInterval(interpTimer);
      interpTimer = null;
      Object.assign(currentValues, targets);
    }
  }, INTERP_MS);

  document.getElementById('time-display').textContent = getTimeLabel(frame);
  document.getElementById('scrubber').value = frame;
  document.getElementById('frame-label').textContent = (frame+1) + ' / ' + getTotalFrames();
}

function getTotalFrames() {
  return mode === 'daily' ? 24 : weekTimestamps.length;
}

function getTimeLabel(frame) {
  if (mode === 'daily') {
    var h = frame;
    var ampm = h < 12 ? 'AM' : 'PM';
    var h12 = h === 0 ? 12 : h > 12 ? h-12 : h;
    return 'Weekday average — ' + h12 + ':00 ' + ampm;
  } else {
    var ts = weekTimestamps[frame];
    var d = new Date(ts);
    var day = DAY_NAMES[d.getDay() === 0 ? 6 : d.getDay()-1];
    var h = d.getHours();
    var ampm = h < 12 ? 'AM' : 'PM';
    var h12 = h === 0 ? 12 : h > 12 ? h-12 : h;
    return day + ' Oct ' + d.getDate() + ' 2025 — ' + h12 + ':00 ' + ampm;
  }
}

function updateScrubber() {
  var total = getTotalFrames();
  document.getElementById('scrubber').max = total - 1;
  document.getElementById('frame-label').textContent = '1 / ' + total;
}

function getDelay() {
  var speed = parseInt(document.getElementById('speed-slider').value);
  return Math.round(1400 - speed * 120);
}

function play() {
  playing = true;
  document.getElementById('play-btn').textContent = 'Pause';
  document.getElementById('play-btn').classList.add('active');
  function step() {
    currentFrame = (currentFrame + 1) % getTotalFrames();
    animateToFrame(currentFrame);
    if (playing) timer = setTimeout(step, getDelay());
  }
  timer = setTimeout(step, getDelay());
}

function pause() {
  playing = false;
  clearTimeout(timer);
  document.getElementById('play-btn').textContent = 'Play';
  document.getElementById('play-btn').classList.remove('active');
}

document.getElementById('play-btn').addEventListener('click', function() {
  playing ? pause() : play();
});
document.getElementById('prev-btn').addEventListener('click', function() {
  pause();
  currentFrame = (currentFrame - 1 + getTotalFrames()) % getTotalFrames();
  animateToFrame(currentFrame);
});
document.getElementById('next-btn').addEventListener('click', function() {
  pause();
  currentFrame = (currentFrame + 1) % getTotalFrames();
  animateToFrame(currentFrame);
});
document.getElementById('scrubber').addEventListener('input', function() {
  pause();
  currentFrame = parseInt(this.value);
  animateToFrame(currentFrame);
});
document.getElementById('mode-select').addEventListener('change', function() {
  pause();
  mode = this.value;
  currentFrame = 0;
  updateScrubber();
  animateToFrame(currentFrame);
});
document.getElementById('show-select').addEventListener('change', function() {
  showMode = this.value;
  document.getElementById('leg-title').textContent =
    showMode === 'ridership' ? 'Ridership level' : 'Congestion level';
  animateToFrame(currentFrame);
});
['borough-filter','tier-filter','lines-filter','line-filter'].forEach(function(id) {
  document.getElementById(id).addEventListener('change', function() {
    animateToFrame(currentFrame);
  });
});

initMarkers();
updateScrubber();
animateToFrame(0);
</script>
</body>
</html>"""

with open('visualizations/animation.html', 'w') as f:
    f.write(html)

print('animation.html generated')