// main.js — shared utilities for TITAN VERITAS

const STATUS_LABELS = {
  registered:       { label: 'Registrato',   cls: 'badge-verified'  },
  data_pending:     { label: 'In attesa',    cls: 'badge-pending'   },
  verified:         { label: 'Verificato',   cls: 'badge-verified'  },
  inactive_football:{ label: 'Inattivo (calcio)', cls: 'badge-inactive' },
  not_eligible:     { label: 'Non eleggibile', cls: 'badge-inactive' },
  archived:         { label: 'Archiviato',   cls: 'badge-inactive'  },
};

const CITIZENSHIP_LABELS = {
  confirmed: { label: '🇸🇲 Cittad. confermata', cls: 'badge-confirmed' },
  to_verify: { label: '🇸🇲 Da verificare',      cls: 'badge-pending'  },
};

const FIFA_LABELS = {
  verified:    { label: 'FIFA ✓', cls: 'badge-verified' },
  to_verify:   { label: 'FIFA ?', cls: 'badge-pending'  },
  not_eligible:{ label: 'FIFA ✗', cls: 'badge-inactive' },
};

const COMMUNITY_STATUS_LABELS = {
  active_dialogue:   { label: 'Dialogo attivo',   cls: 'badge-verified'   },
  primary_amplifier: { label: 'Amplificatore',    cls: 'badge-amplifier'  },
  pending_outreach:  { label: 'Da contattare',    cls: 'badge-pending'    },
  informed:          { label: 'Informata',        cls: 'badge-inactive'   },
};

const COUNTRY_FLAGS = {
  AR: '🇦🇷', US: '🇺🇸', SM: '🇸🇲', IT: '🇮🇹', BR: '🇧🇷',
  DE: '🇩🇪', FR: '🇫🇷', GB: '🇬🇧', ES: '🇪🇸', CH: '🇨🇭',
};

function badge(cfg, key) {
  const info = cfg[key] || { label: key || '—', cls: 'badge-inactive' };
  return `<span class="badge ${info.cls}">${info.label}</span>`;
}

function statusBadge(status)     { return badge(STATUS_LABELS, status); }
function citizenshipBadge(v)     { return badge(CITIZENSHIP_LABELS, v); }
function fifaBadge(v)            { return badge(FIFA_LABELS, v); }
function communityStatusBadge(v) { return badge(COMMUNITY_STATUS_LABELS, v); }

function countryFlag(code) { return COUNTRY_FLAGS[code] || '🌍'; }

function buildGenealogyChip(ancestor, playerCountryCode) {
  if (!ancestor) return '';
  const origin = ancestor.born_in || 'San Marino';
  const year = ancestor.year_emigration ? ` ${ancestor.year_emigration}` : '';
  const rel = ancestor.relation_to_player || ancestor.surname_origin || '—';
  const dest = playerCountryCode || '?';
  return `
    <div class="genealogy-chip">
      🇸🇲 ${origin}${year}
      <span class="sep">→</span>
      ${countryFlag(dest)} ${dest}
      <span class="sep">|</span>
      ${rel}
    </div>`;
}

function formatDate(iso) {
  if (!iso) return '—';
  const [y, m, d] = iso.split('-');
  return `${d}/${m}/${y}`;
}

// Active nav link + mobile nav toggle
document.addEventListener('DOMContentLoaded', () => {
  const page = location.pathname.split('/').pop() || 'index.html';

  document.querySelectorAll('.navbar-nav a').forEach(a => {
    if (a.getAttribute('href') === page || (page === '' && a.getAttribute('href') === 'index.html')) {
      a.classList.add('active');
    }
  });

  // Mobile nav
  const menuBtn = document.getElementById('nav-menu-btn');
  const overlay = document.getElementById('mobile-nav-overlay');
  if (!menuBtn || !overlay) return;

  // Mark active link in overlay
  overlay.querySelectorAll('a').forEach(a => {
    const href = a.getAttribute('href');
    if (href === page || (page === '' && href === 'index.html')) {
      a.classList.add('active');
    }
  });

  function openMenu() {
    overlay.classList.add('open');
    menuBtn.textContent = '✕';
    menuBtn.setAttribute('aria-expanded', 'true');
  }

  function closeMenu() {
    overlay.classList.remove('open');
    menuBtn.textContent = '☰';
    menuBtn.setAttribute('aria-expanded', 'false');
  }

  menuBtn.addEventListener('click', e => {
    e.stopPropagation();
    overlay.classList.contains('open') ? closeMenu() : openMenu();
  });

  overlay.querySelectorAll('a').forEach(a => a.addEventListener('click', closeMenu));

  document.addEventListener('click', e => {
    if (overlay.classList.contains('open') && !overlay.contains(e.target) && e.target !== menuBtn) {
      closeMenu();
    }
  });
});
