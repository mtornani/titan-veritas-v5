// map.js — TitanMap: SVG world map engine

class TitanMap {
  constructor(containerEl, data) {
    this.container = containerEl;
    this.data = data;
    this.svg = null;
    this.svgWidth  = 2000;
    this.svgHeight = 1000;
    this.tooltip = document.getElementById('map-tooltip');
    this.countryStats = null;
  }

  async init() {
    try {
      await this.loadWorldSvg();
      this.countryStats = this.aggregateByCountry();
      this.colorCountries();
      this.drawDiasporaRoutes();
      this.drawPins();
      this.attachEvents();
      document.querySelector('.map-loading')?.remove();
    } catch (e) {
      console.error('Map init failed:', e);
      document.querySelector('.map-loading p').textContent = 'Errore nel caricamento della mappa.';
    }
  }

  async loadWorldSvg() {
    const res = await fetch('assets/img/world.svg');
    if (!res.ok) throw new Error(`SVG fetch failed: ${res.status}`);
    const text = await res.text();
    this.container.innerHTML = text;
    this.svg = this.container.querySelector('svg');
    this.svg.setAttribute('viewBox', `0 0 ${this.svgWidth} ${this.svgHeight}`);
    this.svg.style.cssText = 'width:100%;height:100%;display:block;';
    this.svg.querySelectorAll('path').forEach(p => p.classList.add('country-path'));
  }

  aggregateByCountry() {
    const stats = {};
    const ensure = code => { if (!stats[code]) stats[code] = { communities: [], leads: [] }; };
    for (const c of this.data.communities) { ensure(c.country_code); stats[c.country_code].communities.push(c); }
    for (const l of this.data.leads)       { ensure(l.country_code); stats[l.country_code].leads.push(l); }
    return stats;
  }

  getCountryColor(stats) {
    const hasVerifiedLead = stats.leads.some(l =>
      l.citizenship_sm === 'confirmed' && l.fifa_eligibility === 'verified');
    if (hasVerifiedLead) return '#d4af37';

    const hasActiveComm = stats.communities.some(c =>
      c.status === 'active_dialogue' || c.status === 'primary_amplifier');
    if (hasActiveComm || stats.leads.length > 0) return '#c49a1a';

    if (stats.communities.length > 0) return '#5c5030';
    return null;
  }

  colorCountries() {
    this.svg.querySelectorAll('.country-path').forEach(p => p.setAttribute('fill', '#2a3550'));
    Object.entries(this.countryStats).forEach(([code, stats]) => {
      const el = this.svg.querySelector(`#${code}`);
      if (!el) return;
      const color = this.getCountryColor(stats);
      if (color) el.setAttribute('fill', color);
    });
    const sm = this.svg.querySelector('#SM');
    if (sm) {
      sm.setAttribute('fill', '#d4af37');
      if (sm.tagName === 'circle') { sm.setAttribute('r', '6'); sm.style.filter = 'drop-shadow(0 0 8px #d4af37)'; }
    }
  }

  proj(lat, lng) {
    return {
      x: (lng + 180) * (this.svgWidth  / 360),
      y: (90 - lat)  * (this.svgHeight / 180),
    };
  }

  curvedPath(from, to) {
    const midX = (from.x + to.x) / 2;
    const midY = (from.y + to.y) / 2 - Math.abs(to.x - from.x) * 0.18;
    return `M ${from.x.toFixed(1)} ${from.y.toFixed(1)} Q ${midX.toFixed(1)} ${midY.toFixed(1)} ${to.x.toFixed(1)} ${to.y.toFixed(1)}`;
  }

  drawDiasporaRoutes() {
    const ns = 'http://www.w3.org/2000/svg';
    const layer = document.createElementNS(ns, 'g');
    layer.setAttribute('class', 'diaspora-routes-layer');
    this.svg.appendChild(layer);
    const origin = this.proj(this.data.origin.lat, this.data.origin.lng);
    const drawn = new Set();
    for (const c of this.data.communities) {
      const key = `${c.lat},${c.lng}`;
      if (drawn.has(key)) continue;
      drawn.add(key);
      const dest = this.proj(c.lat, c.lng);
      const path = document.createElementNS(ns, 'path');
      path.setAttribute('d', this.curvedPath(origin, dest));
      const isActive = c.status === 'active_dialogue' || c.status === 'primary_amplifier';
      path.setAttribute('class', `diaspora-route${isActive ? ' route-active' : ''}`);
      path.style.strokeDashoffset = Math.random() * -50;
      layer.appendChild(path);
    }
  }

  pinRadius(communityList) {
    if (communityList.some(c => c.status === 'primary_amplifier')) return 10;
    if (communityList.some(c => c.status === 'active_dialogue'))   return 8;
    return 6;
  }

  pinClass(communityList) {
    if (communityList.some(c => c.status === 'primary_amplifier')) return 'pin-amplifier';
    if (communityList.some(c => c.status === 'active_dialogue'))   return 'pin-active';
    return 'pin-pending';
  }

  drawPins() {
    const ns = 'http://www.w3.org/2000/svg';
    const layer = document.createElementNS(ns, 'g');
    layer.setAttribute('class', 'pins-layer');
    this.svg.appendChild(layer);

    const cityGroups = {};
    for (const c of this.data.communities) {
      const key = `${c.lat},${c.lng}`;
      if (!cityGroups[key]) cityGroups[key] = { lat: c.lat, lng: c.lng, communities: [] };
      cityGroups[key].communities.push(c);
    }

    for (const [, group] of Object.entries(cityGroups)) {
      const pt  = this.proj(group.lat, group.lng);
      const r   = this.pinRadius(group.communities);
      const cls = this.pinClass(group.communities);
      const cityName = group.communities[0].city || group.communities[0].name;

      const g = document.createElementNS(ns, 'g');
      g.setAttribute('class', `pin-group ${cls}`);
      g.setAttribute('data-lat',     group.lat);
      g.setAttribute('data-lng',     group.lng);
      g.setAttribute('data-city',    cityName || '');
      g.setAttribute('data-country', group.communities[0].country_code || '');
      g.style.cursor = 'pointer';

      const pulse = document.createElementNS(ns, 'circle');
      pulse.setAttribute('cx', pt.x.toFixed(1)); pulse.setAttribute('cy', pt.y.toFixed(1));
      pulse.setAttribute('r', r + 2); pulse.setAttribute('class', 'pin-pulse');
      pulse.setAttribute('fill', 'transparent');

      const circle = document.createElementNS(ns, 'circle');
      circle.setAttribute('cx', pt.x.toFixed(1)); circle.setAttribute('cy', pt.y.toFixed(1));
      circle.setAttribute('r', r); circle.setAttribute('class', 'pin-circle');

      g.appendChild(pulse); g.appendChild(circle); layer.appendChild(g);
    }

    const smPt = this.proj(this.data.origin.lat, this.data.origin.lng);
    const smG  = document.createElementNS(ns, 'g');
    smG.setAttribute('class', 'pin-group pin-origin');
    smG.setAttribute('data-country', 'SM');
    smG.setAttribute('data-city', 'San Marino');
    smG.style.cursor = 'pointer';

    const smPulse  = document.createElementNS(ns, 'circle');
    smPulse.setAttribute('cx', smPt.x.toFixed(1)); smPulse.setAttribute('cy', smPt.y.toFixed(1));
    smPulse.setAttribute('r', '10'); smPulse.setAttribute('class', 'pin-pulse');

    const smCircle = document.createElementNS(ns, 'circle');
    smCircle.setAttribute('cx', smPt.x.toFixed(1)); smCircle.setAttribute('cy', smPt.y.toFixed(1));
    smCircle.setAttribute('r', '7'); smCircle.setAttribute('class', 'pin-circle');

    smG.appendChild(smPulse); smG.appendChild(smCircle); layer.appendChild(smG);
  }

  attachEvents() {
    this.svg.querySelectorAll('.country-path').forEach(path => {
      const id = path.getAttribute('id');
      if (!id) return;
      path.addEventListener('mouseenter', e => this.onCountryHover(e, id, path));
      path.addEventListener('mousemove',  e => this.onTooltipMove(e));
      path.addEventListener('mouseleave', () => this.hideTooltip());
      path.addEventListener('click',      () => PanelController.openCountry(id, this.countryStats, this.data));
    });

    this.svg.querySelectorAll('.pin-group').forEach(pin => {
      const country = pin.getAttribute('data-country');
      const city    = pin.getAttribute('data-city');
      pin.addEventListener('mouseenter', e => this.onPinHover(e, country, city));
      pin.addEventListener('mousemove',  e => this.onTooltipMove(e));
      pin.addEventListener('mouseleave', () => this.hideTooltip());
      pin.addEventListener('click', e => {
        e.stopPropagation();
        PanelController.openCountry(country, this.countryStats, this.data, city);
      });
    });

    this.container.addEventListener('click', e => {
      if (!e.target.closest('.country-path') && !e.target.closest('.pin-group')) PanelController.close();
    });
  }

  onCountryHover(e, id, path) {
    if (!this.tooltip) return;
    const stats = this.countryStats[id];
    if (!stats) return;
    const nComm = stats.communities.length;
    const nLead = stats.leads.length;
    if (!nComm && !nLead) return;
    const countryName = stats.communities[0]?.country || id;
    this.tooltip.innerHTML = `
      <div class="tooltip-country">${countryFlag(id)} ${countryName}</div>
      <div class="tooltip-stats">
        ${nComm ? `<span class="tooltip-stat">🏛 ${nComm} comunità</span>` : ''}
        ${nLead ? `<span class="tooltip-stat">⚽ ${nLead} giocatori</span>` : ''}
      </div>`;
    this.tooltip.style.display = 'block';
    this.onTooltipMove(e);
  }

  onPinHover(e, country, city) {
    if (!this.tooltip) return;
    const stats = this.countryStats[country];
    const comms = stats?.communities.filter(c => !city || c.city === city || !c.city) || [];
    const leads = stats?.leads.filter(l => !city || l.city === city || !l.city) || [];
    const flag  = countryFlag(country);
    this.tooltip.innerHTML = `
      <div class="tooltip-country">${flag} ${city || country}</div>
      <div class="tooltip-stats">
        ${comms.length ? `<span class="tooltip-stat">🏛 ${comms.length} comunità</span>` : ''}
        ${leads.length ? `<span class="tooltip-stat">⚽ ${leads.length} giocatori</span>` : ''}
      </div>`;
    this.tooltip.style.display = 'block';
    this.onTooltipMove(e);
  }

  onTooltipMove(e) {
    if (!this.tooltip) return;
    const rect = this.container.getBoundingClientRect();
    this.tooltip.style.left = `${e.clientX - rect.left}px`;
    this.tooltip.style.top  = `${e.clientY - rect.top}px`;
  }

  hideTooltip() {
    if (this.tooltip) this.tooltip.style.display = 'none';
  }
}

function countryFlag(code) {
  const flags = { AR:'🇦🇷', US:'🇺🇸', SM:'🇸🇲', IT:'🇮🇹', BR:'🇧🇷', DE:'🇩🇪', FR:'🇫🇷', GB:'🇬🇧', ES:'🇪🇸' };
  return flags[code] || '🌍';
}
