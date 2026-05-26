// panel.js — Side panel controller for TITAN VERITAS

const PanelController = (() => {
  const panel    = document.getElementById('side-panel');
  const backdrop = document.getElementById('panel-backdrop');
  let   activeCountry = null;
  let   activeTab     = 'communities';

  function open() {
    panel?.classList.add('open');
    backdrop?.classList.add('active');
  }

  function close() {
    panel?.classList.remove('open');
    backdrop?.classList.remove('active');
    document.querySelectorAll('.country-path.country-selected').forEach(el => {
      el.classList.remove('country-selected');
    });
    activeCountry = null;
  }

  function setTab(tab) {
    activeTab = tab;
    document.querySelectorAll('.panel-tab').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.tab === tab);
    });
    document.querySelectorAll('.panel-tab-content').forEach(el => {
      el.classList.toggle('active', el.dataset.tabContent === tab);
    });
  }

  function openCountry(countryCode, countryStats, data, filterCity) {
    if (!panel) return;
    if (countryCode === 'SM') {
      renderOriginPanel(data);
      open();
      return;
    }
    const stats = countryStats[countryCode];
    if (!stats) return;
    activeCountry = countryCode;
    document.querySelectorAll('.country-path.country-selected').forEach(el => el.classList.remove('country-selected'));
    document.getElementById(countryCode)?.classList.add('country-selected');
    let communities = filterCity ? stats.communities.filter(c => !c.city || c.city === filterCity) : stats.communities;
    let leads = filterCity ? stats.leads.filter(l => !l.city || l.city === filterCity) : stats.leads;
    renderPanel(countryCode, communities, leads, data);
    open();
    setTab(leads.length ? 'leads' : 'communities');
  }

  function renderPanel(countryCode, communities, leads, data) {
    const flag = countryFlag(countryCode);
    const name = communities[0]?.country || countryCode;
    document.getElementById('panel-flag').textContent = flag;
    document.getElementById('panel-country-name').textContent = name;
    document.getElementById('panel-country-sub').textContent = `${communities.length} comunità · ${leads.length} lead`;
    document.getElementById('panel-communities').innerHTML = communities.length
      ? communities.map(renderCommunityCard).join('')
      : '<p style="color:var(--text-secondary);font-size:.82rem;text-align:center;padding:1rem 0">Nessuna comunità registrata.</p>';
    document.getElementById('panel-leads').innerHTML = leads.length
      ? leads.map(renderLeadCard).join('')
      : '<p style="color:var(--text-secondary);font-size:.82rem;text-align:center;padding:1rem 0">Nessun lead per questo filtro.</p>';
  }

  function renderOriginPanel(data) {
    const countries = new Set(data.communities.map(c => c.country_code)).size;
    document.getElementById('panel-flag').textContent = '🇸🇲';
    document.getElementById('panel-country-name').textContent = 'San Marino';
    document.getElementById('panel-country-sub').textContent = 'Origine — La Serenissima Repubblica';
    document.getElementById('panel-communities').innerHTML = `
      <div class="origin-panel-content">
        <div class="origin-flag">🇸🇲</div>
        <div class="origin-title">Progetto Titan Veritas</div>
        <div class="origin-sub">Ricerca indipendente sulla diaspora sammarinese</div>
        <div class="origin-stats-grid">
          <div class="origin-stat-card"><div class="origin-stat-val">${data.metadata.total_leads}</div><div class="origin-stat-lbl">Lead totali</div></div>
          <div class="origin-stat-card"><div class="origin-stat-val">${data.metadata.active_leads}</div><div class="origin-stat-lbl">Lead attivi</div></div>
          <div class="origin-stat-card"><div class="origin-stat-val">${data.communities.length}</div><div class="origin-stat-lbl">Comunità</div></div>
          <div class="origin-stat-card"><div class="origin-stat-val">${countries}</div><div class="origin-stat-lbl">Paesi</div></div>
        </div>
        <p style="font-size:.78rem;color:var(--text-secondary);text-align:left">Stato: <strong style="color:var(--gold)">${data.metadata.project_status.replace(/_/g,' ')}</strong><br>Aggiornato: ${data.metadata.last_updated}</p>
      </div>`;
    document.getElementById('panel-leads').innerHTML = '';
    setTab('communities');
  }

  function renderCommunityCard(c) {
    const statusLabel = {active_dialogue:['Dialogo attivo','badge-verified'],primary_amplifier:['Amplificatore','badge-amplifier'],pending_outreach:['Da contattare','badge-pending'],informed:['Informata','badge-inactive']}[c.status]||[c.status,'badge-inactive'];
    const membersTxt = c.members_estimated ? `${c.members_estimated.toLocaleString('it-IT')} iscritti est.` : '';
    return `<div class="panel-community-card"><div class="panel-community-name">${escHtml(c.name)}</div><div class="panel-community-meta">${c.city?`<span>📍 ${escHtml(c.city)}</span>`:''} ${c.president?`<span>👤 Pres. ${escHtml(c.president)}</span>`:''} ${membersTxt?`<span>👥 ${membersTxt}</span>`:''} ${c.first_contact?`<span>📅 Primo contatto: ${formatDate(c.first_contact)}</span>`:''}</div><span class="badge ${statusLabel[1]}">${statusLabel[0]}</span></div>`;
  }

  function renderLeadCard(l) {
    const citizenshipInfo = {confirmed:{label:'🇸🇲 Cittad. confermata',cls:'badge-confirmed'},to_verify:{label:'🇸🇲 Da verificare',cls:'badge-pending'}}[l.citizenship_sm]||{label:'?',cls:'badge-inactive'};
    const fifaInfo = {verified:{label:'FIFA ✓',cls:'badge-verified'},to_verify:{label:'FIFA ?',cls:'badge-pending'},not_eligible:{label:'FIFA ✗',cls:'badge-inactive'}}[l.fifa_eligibility]||{label:'?',cls:'badge-inactive'};
    const statusInfo = {registered:{label:'Registrato',cls:'badge-verified'},data_pending:{label:'In attesa dati',cls:'badge-pending'},verified:{label:'Verificato',cls:'badge-verified'},inactive_football:{label:'Inattivo',cls:'badge-inactive'}}[l.status]||{label:l.status,cls:'badge-inactive'};
    const ancestor = l.ancestor;
    const geneaHtml = ancestor ? `<div class="panel-genealogy">🇸🇲 ${escHtml(ancestor.born_in||'San Marino')}${ancestor.year_emigration?` ${ancestor.year_emigration}`:''} → 🇦🇷 ${escHtml(l.country_code)} · <em>${escHtml(ancestor.relation_to_player||ancestor.surname_origin||'?')}</em></div>` : '';
    return `<div class="panel-lead-card"><div class="panel-lead-alias">${escHtml(l.alias)}</div><div class="panel-lead-meta">${l.year_of_birth?`<span>📅 Nato/a nel ${l.year_of_birth}</span>`:''} ${l.city?`<span>📍 ${escHtml(l.city)}</span>`:''} ${l.club?`<span>⚽ ${escHtml(l.club)}</span>`:''} ${l.position?`<span>🎯 ${escHtml(l.position)}</span>`:''}<span>📡 ${escHtml(l.source||'')}</span></div><div class="panel-lead-badges"><span class="badge ${statusInfo.cls}">${statusInfo.label}</span> <span class="badge ${citizenshipInfo.cls}">${citizenshipInfo.label}</span> <span class="badge ${fifaInfo.cls}">${fifaInfo.label}</span></div>${geneaHtml}</div>`;
  }

  function escHtml(str) {
    if (!str) return '';
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function formatDate(iso) {
    if (!iso) return '—';
    const [y,m,d] = iso.split('-');
    return `${d}/${m}/${y}`;
  }

  function countryFlag(code) {
    return {AR:'🇦🇷',US:'🇺🇸',SM:'🇸🇲',IT:'🇮🇹',BR:'🇧🇷',DE:'🇩🇪',FR:'🇫🇷',GB:'🇬🇧',ES:'🇪🇸'}[code]||'🌍';
  }

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('panel-close')?.addEventListener('click', close);
    backdrop?.addEventListener('click', close);
    document.querySelectorAll('.panel-tab').forEach(btn => {
      btn.addEventListener('click', () => setTab(btn.dataset.tab));
    });
  });

  return { open, close, openCountry };
})();