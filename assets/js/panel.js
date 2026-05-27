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

    // Clear selected country highlights
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

    // Special case: origin
    if (countryCode === 'SM') {
      renderOriginPanel(data);
      open();
      return;
    }

    const stats = countryStats[countryCode];
    if (!stats) return;

    activeCountry = countryCode;

    // Highlight on map
    document.querySelectorAll('.country-path.country-selected').forEach(el => {
      el.classList.remove('country-selected');
    });
    const path = document.getElementById(countryCode);
    path?.classList.add('country-selected');

    let communities = stats.communities;
    let leads       = stats.leads;

    if (filterCity) {
      communities = communities.filter(c => !c.city || c.city === filterCity);
      leads       = leads.filter(l => !l.city || l.city === filterCity);
    }

    renderPanel(countryCode, communities, leads, data);
    open();
    setTab(leads.length ? 'leads' : 'communities');
  }

  function renderPanel(countryCode, communities, leads, data) {
    const flag = countryFlag(countryCode);
    const name = communities[0]?.country || countryCode;

    // Header
    document.getElementById('panel-flag').textContent  = flag;
    document.getElementById('panel-country-name').textContent = name;
    document.getElementById('panel-country-sub').textContent  =
      `${communities.length} comunità · ${leads.length} giocatori`;

    // Communities tab
    const commContainer = document.getElementById('panel-communities');
    commContainer.innerHTML = communities.length
      ? communities.map(c => renderCommunityCard(c)).join('')
      : '<p style="color:var(--text-secondary);font-size:.82rem;text-align:center;padding:1rem 0">Nessuna comunità registrata.</p>';

    // Leads tab
    const leadsContainer = document.getElementById('panel-leads');
    leadsContainer.innerHTML = leads.length
      ? leads.map(l => renderLeadCard(l)).join('')
      : '<p style="color:var(--text-secondary);font-size:.82rem;text-align:center;padding:1rem 0">Nessun giocatore trovato.</p>';
  }

  function renderOriginPanel(data) {
    const totalLeads = data.metadata.total_leads;
    const activeLeads = data.metadata.active_leads;
    const totalComm   = data.communities.length;
    const countries   = new Set(data.communities.map(c => c.country_code)).size;

    // Hide tabs, show origin content
    document.getElementById('panel-flag').textContent = '🇸🇲';
    document.getElementById('panel-country-name').textContent = 'San Marino';
    document.getElementById('panel-country-sub').textContent  = 'Origine — La Serenissima Repubblica';

    const commContainer = document.getElementById('panel-communities');
    commContainer.innerHTML = `
      <div class="origin-panel-content">
        <div class="origin-flag">🇸🇲</div>
        <div class="origin-title">Progetto Titan Veritas</div>
        <div class="origin-sub">Ricerca indipendente sulla diaspora sammarinese</div>
        <div class="origin-stats-grid">
          <div class="origin-stat-card">
            <div class="origin-stat-val">${totalLeads}</div>
            <div class="origin-stat-lbl">Lead totali</div>
          </div>
          <div class="origin-stat-card">
            <div class="origin-stat-val">${activeLeads}</div>
            <div class="origin-stat-lbl">Lead attivi</div>
          </div>
          <div class="origin-stat-card">
            <div class="origin-stat-val">${totalComm}</div>
            <div class="origin-stat-lbl">Comunità</div>
          </div>
          <div class="origin-stat-card">
            <div class="origin-stat-val">${countries}</div>
            <div class="origin-stat-lbl">Paesi</div>
          </div>
        </div>
        <p style="font-size:.78rem;color:var(--text-secondary);text-align:left">
          Stato progetto: <strong style="color:var(--gold)">${data.metadata.project_status.replace(/_/g,' ')}</strong>
          <br>Aggiornato: ${data.metadata.last_updated}
        </p>
      </div>`;

    const leadsContainer = document.getElementById('panel-leads');
    leadsContainer.innerHTML = '';

    setTab('communities');
  }

  function renderCommunityCard(c) {
    const statusLabel = {
      active_dialogue:   ['Dialogo attivo',  'badge-verified'],
      primary_amplifier: ['Amplificatore',   'badge-amplifier'],
      pending_outreach:  ['Da contattare',   'badge-pending'],
      informed:          ['Informata',       'badge-inactive'],
    }[c.status] || [c.status, 'badge-inactive'];

    const membersTxt = c.members_estimated ? `${c.members_estimated.toLocaleString('it-IT')} iscritti est.` : '';
    const presidentTxt = c.president ? `Pres. ${c.president}` : '';
    const contactTxt   = c.first_contact ? `Primo contatto: ${formatDate(c.first_contact)}` : '';

    return `
      <div class="panel-community-card">
        <div class="panel-community-name">${escHtml(c.name)}</div>
        <div class="panel-community-meta">
          ${c.city ? `<span>📍 ${escHtml(c.city)}</span>` : ''}
          ${presidentTxt ? `<span>👤 ${escHtml(presidentTxt)}</span>` : ''}
          ${membersTxt   ? `<span>👥 ${membersTxt}</span>` : ''}
          ${contactTxt   ? `<span>📅 ${contactTxt}</span>` : ''}
        </div>
        <span class="badge ${statusLabel[1]}">${statusLabel[0]}</span>
      </div>`;
  }

  function renderLeadCard(l) {
    const citizenshipInfo = {
      confirmed: { label: '🇸🇲 Cittadino SM',   cls: 'badge-confirmed' },
      to_verify: { label: '🇸🇲 Da verificare',  cls: 'badge-pending'  },
    }[l.citizenship_sm] || { label: '?', cls: 'badge-inactive' };

    const fifaInfo = {
      verified:               { label: 'FIFA: eleggibile',     cls: 'badge-verified' },
      to_verify:              { label: 'FIFA: da verificare',  cls: 'badge-pending'  },
      to_verify_complex_case: { label: 'FIFA: caso speciale',  cls: 'badge-pending'  },
      not_eligible:           { label: 'FIFA: non eleggibile', cls: 'badge-inactive' },
    }[l.fifa_eligibility] || { label: 'FIFA: ?', cls: 'badge-inactive' };

    const statusInfo = {
      registered:        { label: 'Registrato',     cls: 'badge-verified' },
      data_pending:      { label: 'Dati mancanti',  cls: 'badge-pending'  },
      verified:          { label: 'Verificato',     cls: 'badge-verified' },
      inactive_football: { label: 'Non gioca più',  cls: 'badge-inactive' },
    }[l.status] || { label: l.status, cls: 'badge-inactive' };

    const ancestor = l.ancestor;
    let geneaHtml = '';
    if (ancestor) {
      const origin = ancestor.born_in || 'San Marino';
      const year   = ancestor.year_emigration ? ` ${ancestor.year_emigration}` : '';
      const rel    = ancestor.relation_to_player || ancestor.surname_origin || '?';
      geneaHtml = `
        <div class="panel-genealogy">
          🇸🇲 ${escHtml(origin)}${year} → 🇦🇷 ${escHtml(l.country_code)} · <em>${escHtml(rel)}</em>
        </div>`;
    }

    return `
      <div class="panel-lead-card">
        <div class="panel-lead-alias">${escHtml(l.alias)}</div>
        <div class="panel-lead-meta">
          ${l.year_of_birth  ? `<span>📅 Nato/a nel ${l.year_of_birth}</span>` : ''}
          ${l.city           ? `<span>📍 ${escHtml(l.city)}</span>` : ''}
          ${l.club           ? `<span>⚽ ${escHtml(l.club)}</span>` : ''}
          ${l.position       ? `<span>🎯 ${escHtml(l.position)}</span>` : ''}
          <span>📡 ${escHtml(l.source || '')}</span>
        </div>
        <div class="panel-lead-badges">
          <span class="badge ${statusInfo.cls}">${statusInfo.label}</span>
          <span class="badge ${citizenshipInfo.cls}">${citizenshipInfo.label}</span>
          <span class="badge ${fifaInfo.cls}">${fifaInfo.label}</span>
        </div>
        ${geneaHtml}
      </div>`;
  }

  function escHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function formatDate(iso) {
    if (!iso) return '—';
    const [y, m, d] = iso.split('-');
    return `${d}/${m}/${y}`;
  }

  function countryFlag(code) {
    const flags = { AR:'🇦🇷', US:'🇺🇸', SM:'🇸🇲', IT:'🇮🇹', BR:'🇧🇷', DE:'🇩🇪', FR:'🇫🇷', GB:'🇬🇧', ES:'🇪🇸' };
    return flags[code] || '🌍';
  }

  // Wire up static DOM events
  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('panel-close')?.addEventListener('click', close);
    backdrop?.addEventListener('click', close);

    document.querySelectorAll('.panel-tab').forEach(btn => {
      btn.addEventListener('click', () => setTab(btn.dataset.tab));
    });
  });

  return { open, close, openCountry };
})();
