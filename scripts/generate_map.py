import json

with open('visualizations/station_data.json') as f:
    data = json.load(f)

html = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>MTA Subway Congestion Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, sans-serif; background: #f8f8f8; }
#controls { display: flex; flex-wrap: wrap; gap: 10px; padding: 12px 16px; background: white; border-bottom: 1px solid #e0e0e0; align-items: center; }
#controls label { font-size: 13px; color: #666; }
#controls select { font-size: 13px; padding: 4px 8px; border: 1px solid #ccc; border-radius: 6px; background: white; }
#map { height: calc(100vh - 52px); }
#legend { position: absolute; bottom: 30px; left: 16px; z-index: 1000; background: white; padding: 10px 14px; border-radius: 8px; border: 1px solid #ddd; font-size: 12px; }
.leg-row { display: flex; align-items: center; gap: 6px; margin: 3px 0; }
.dot { width: 10px; height: 10px; border-radius: 50%; }
#title { font-size: 14px; font-weight: 600; margin-right: 8px; color: #333; }
</style>
</head>
<body>
<div id="controls">
  <span id="title">MTA Subway Congestion</span>
  <label>Color by <select id="color-mode">
    <option value="congestion">Congestion rate</option>
    <option value="drift">Drift (2023 to 2024)</option>
    <option value="auc">Model AUC</option>
  </select></label>
  <label>Borough <select id="borough-filter">
    <option value="all">All boroughs</option>
    <option value="Manhattan">Manhattan</option>
    <option value="Brooklyn">Brooklyn</option>
    <option value="Queens">Queens</option>
    <option value="Bronx">Bronx</option>
    <option value="Staten Island">Staten Island</option>
  </select></label>
  <label>Tier <select id="tier-filter">
    <option value="all">All tiers</option>
    <option value="4">Tier 4 (largest)</option>
    <option value="3">Tier 3</option>
    <option value="2">Tier 2</option>
    <option value="1">Tier 1 (smallest)</option>
  </select></label>
  <label>Lines <select id="lines-filter">
    <option value="all">Any</option>
    <option value="1">1 line</option>
    <option value="2">2 lines</option>
    <option value="3+">3+ lines</option>
  </select></label>
</div>
<div id="map"></div>
<div id="legend">
  <div style="font-weight:600;margin-bottom:6px;font-size:12px" id="leg-title">Congestion rate</div>
  <div class="leg-row"><span class="dot" style="background:#185FA5"></span>Low</div>
  <div class="leg-row"><span class="dot" style="background:#EF9F27"></span>Medium</div>
  <div class="leg-row"><span class="dot" style="background:#E24B4A"></span>High</div>
</div>
<script>
var STATIONS = """ + json.dumps(data) + """;

function lerp(a, b, t) { return a + (b-a)*t; }

function getColor(s, mode) {
  var t;
  if (mode === 'congestion') {
    t = Math.min(1, Math.max(0, (s.congestion_rate - 0.10) / 0.32));
  } else if (mode === 'drift') {
    t = Math.min(1, Math.max(0, (s.drift + 0.10) / 0.22));
  } else {
    t = Math.min(1, Math.max(0, 1 - (s.auc - 0.92) / 0.07));
  }
  var r, g, b;
  if (t < 0.5) {
    var s2 = t*2;
    r = Math.round(lerp(24,239,s2)); g = Math.round(lerp(95,159,s2)); b = Math.round(lerp(165,39,s2));
  } else {
    var s2 = (t-0.5)*2;
    r = Math.round(lerp(239,226,s2)); g = Math.round(lerp(159,75,s2)); b = Math.round(lerp(39,74,s2));
  }
  return 'rgb('+r+','+g+','+b+')';
}

var map = L.map('map').setView([40.730, -73.935], 11);
L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
  attribution: '&copy; OpenStreetMap &copy; CARTO',
  maxZoom: 18
}).addTo(map);

var markers = [];

function render() {
  markers.forEach(function(m) { map.removeLayer(m); });
  markers = [];
  var mode = document.getElementById('color-mode').value;
  var borough = document.getElementById('borough-filter').value;
  var tier = document.getElementById('tier-filter').value;
  var lines = document.getElementById('lines-filter').value;
  document.getElementById('leg-title').textContent =
    mode === 'congestion' ? 'Congestion rate' :
    mode === 'drift' ? 'Drift 2023 to 2024' : 'Model AUC';

  STATIONS.forEach(function(s) {
    if (borough !== 'all' && s.borough !== borough) return;
    if (tier !== 'all' && s.station_tier !== parseInt(tier)) return;
    if (lines === '1' && s.num_lines !== 1) return;
    if (lines === '2' && s.num_lines !== 2) return;
    if (lines === '3+' && s.num_lines < 3) return;

    var color = getColor(s, mode);
    var radius = 4 + s.station_tier * 1.5;
    var m = L.circleMarker([s.lat, s.lon], {
      radius: radius,
      fillColor: color,
      color: '#fff',
      weight: 1.5,
      fillOpacity: 0.85
    }).addTo(map);

    var drift = (s.drift >= 0 ? '+' : '') + (s.drift*100).toFixed(1) + '%';
    var crate = (s.congestion_rate*100).toFixed(1) + '%';
    var auc = s.auc ? s.auc.toFixed(4) : 'N/A';

    m.bindPopup(
      '<b style="font-size:13px">' + s.name + '</b>' +
      '<table style="font-size:12px;margin-top:6px;border-collapse:collapse">' +
      '<tr><td style="color:#888;padding:2px 8px 2px 0">Borough</td><td>' + s.borough + '</td></tr>' +
      '<tr><td style="color:#888;padding:2px 8px 2px 0">Congestion rate</td><td>' + crate + '</td></tr>' +
      '<tr><td style="color:#888;padding:2px 8px 2px 0">Drift</td><td>' + drift + '</td></tr>' +
      '<tr><td style="color:#888;padding:2px 8px 2px 0">Model AUC</td><td>' + auc + '</td></tr>' +
      '<tr><td style="color:#888;padding:2px 8px 2px 0">Lines</td><td>' + s.num_lines + '</td></tr>' +
      '<tr><td style="color:#888;padding:2px 8px 2px 0">Station tier</td><td>' + s.station_tier + ' / 4</td></tr>' +
      '</table>',
      {maxWidth: 260}
    );
    markers.push(m);
  });
}

render();
['color-mode','borough-filter','tier-filter','lines-filter'].forEach(function(id) {
  document.getElementById(id).addEventListener('change', render);
});
</script>
</body>
</html>"""

with open('visualizations/map.html', 'w') as f:
    f.write(html)

print('map.html generated with', len(data), 'stations')