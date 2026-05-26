#!/usr/bin/env python3
"""Generate world.svg from world-atlas topojson with ISO alpha-2 IDs."""
import json, math, urllib.request

# ISO 3166-1 numeric -> alpha-2 mapping (comprehensive)
NUM_TO_A2 = {
    4:"AF",8:"AL",12:"DZ",24:"AO",32:"AR",36:"AU",40:"AT",50:"BD",56:"BE",
    64:"BT",68:"BO",76:"BR",100:"BG",104:"MM",116:"KH",120:"CM",124:"CA",
    140:"CF",152:"CL",156:"CN",170:"CO",180:"CD",191:"HR",192:"CU",196:"CY",
    203:"CZ",208:"DK",214:"DO",218:"EC",818:"EG",231:"ET",246:"FI",250:"FR",
    266:"GA",276:"DE",288:"GH",300:"GR",320:"GT",332:"HT",340:"HN",348:"HU",
    356:"IN",360:"ID",364:"IR",368:"IQ",372:"IE",376:"IL",380:"IT",388:"JM",
    392:"JP",400:"JO",398:"KZ",404:"KE",408:"KP",410:"KR",414:"KW",418:"LA",
    422:"LB",430:"LR",434:"LY",458:"MY",484:"MX",504:"MA",508:"MZ",516:"NA",
    524:"NP",528:"NL",540:"NC",554:"NZ",558:"NI",562:"NE",566:"NG",578:"NO",
    586:"PK",591:"PA",598:"PG",600:"PY",604:"PE",608:"PH",616:"PL",620:"PT",
    634:"QA",642:"RO",643:"RU",646:"RW",682:"SA",686:"SN",706:"SO",710:"ZA",
    724:"ES",728:"SS",729:"SD",752:"SE",756:"CH",760:"SY",764:"TH",792:"TR",
    800:"UG",804:"UA",784:"AE",826:"GB",840:"US",858:"UY",862:"VE",704:"VN",
    887:"YE",894:"ZM",716:"ZW",12:"DZ",51:"AM",31:"AZ",112:"BY",242:"FJ",
    268:"GE",616:"PL",466:"ML",478:"MR",472:"MV",480:"MU",496:"MN",499:"ME",
    807:"MK",450:"MG",454:"MW",446:"MO",414:"KW",417:"KG",440:"LT",428:"LV",
    422:"LB",426:"LS",434:"LY",430:"LR",144:"LK",694:"SL",662:"LC",666:"PM",
    659:"KN",670:"VC",780:"TT",798:"TV",800:"UG",826:"GB",840:"US",
    # SM needs special treatment (too small for 110m dataset)
    674:"SM",
}

SVG_W = 2000
SVG_H = 1000

def decode_topo(topo):
    transform = topo.get("transform", {})
    scale = transform.get("scale", [1, 1])
    translate = transform.get("translate", [0, 0])
    arcs_raw = topo["arcs"]

    arcs = []
    for arc in arcs_raw:
        points = []
        x, y = 0, 0
        for dp in arc:
            x += dp[0]
            y += dp[1]
            lon = x * scale[0] + translate[0]
            lat = y * scale[1] + translate[1]
            points.append((lon, lat))
        arcs.append(points)
    return arcs

def proj(lon, lat):
    x = (lon + 180) / 360 * SVG_W
    y = (90 - lat) / 180 * SVG_H
    return x, y

def arcs_to_path(arc_indices, arcs):
    parts = []
    for idx in arc_indices:
        reverse = idx < 0
        arc = arcs[~idx if reverse else idx]
        if reverse:
            arc = list(reversed(arc))
        pts = [proj(lon, lat) for lon, lat in arc]
        if not pts:
            continue
        cmd = f"M {pts[0][0]:.2f},{pts[0][1]:.2f}"
        for px, py in pts[1:]:
            cmd += f" L {px:.2f},{py:.2f}"
        parts.append(cmd)
    return " ".join(parts) + " Z"

def geometry_to_path(geom, arcs):
    gtype = geom["type"]
    paths = []
    if gtype == "Polygon":
        for ring in geom["arcs"]:
            paths.append(arcs_to_path(ring, arcs))
    elif gtype == "MultiPolygon":
        for poly in geom["arcs"]:
            for ring in poly:
                paths.append(arcs_to_path(ring, arcs))
    return " ".join(paths)

def main():
    url = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json"
    print(f"Fetching {url} ...")
    with urllib.request.urlopen(url, timeout=30) as r:
        topo = json.load(r)
    print("Decoding arcs...")
    arcs = decode_topo(topo)

    geometries = topo["objects"]["countries"]["geometries"]
    print(f"Processing {len(geometries)} geometries...")

    paths_by_id = {}
    unknown = []
    for geom in geometries:
        num_id = int(geom.get("id", 0))
        a2 = NUM_TO_A2.get(num_id)
        if not a2:
            unknown.append(num_id)
            a2 = f"_{num_id}"
        d = geometry_to_path(geom, arcs)
        if a2 in paths_by_id:
            paths_by_id[a2] += " " + d
        else:
            paths_by_id[a2] = d

    if unknown:
        print(f"  {len(unknown)} unmapped numeric IDs: {unknown[:20]}...")

    # Build SVG
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {SVG_W} {SVG_H}" '
        f'width="{SVG_W}" height="{SVG_H}" id="world-map">',
        '  <rect width="2000" height="1000" fill="#0a0e1a"/>',
    ]
    for a2, d in sorted(paths_by_id.items()):
        if not d.strip():
            continue
        lines.append(f'  <path id="{a2}" d="{d}" fill="#1e2536" stroke="#0a0e1a" stroke-width="0.5"/>')

    # San Marino is too small for 110m — add as a circle marker
    sm_x, sm_y = proj(12.4578, 43.9424)
    lines.append(
        f'  <circle id="SM" cx="{sm_x:.2f}" cy="{sm_y:.2f}" r="4" '
        f'fill="#d4af37" stroke="#d4af37" stroke-width="1"/>'
    )
    lines.append('</svg>')

    svg_content = "\n".join(lines)
    out_path = "assets/img/world.svg"
    with open(out_path, "w") as f:
        f.write(svg_content)
    print(f"Written {out_path} ({len(svg_content):,} bytes, {len(paths_by_id)} countries)")

if __name__ == "__main__":
    main()
