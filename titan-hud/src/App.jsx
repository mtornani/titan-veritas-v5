import React, { useState, useMemo, useRef } from 'react';
import playersData from './data.json';

const COUNTRY_LABELS_IT = { 'Argentina': 'Argentina', 'France': 'Francia', 'Chile': 'Cile', 'Unknown': 'Sconosciuto' };

// ─── Position map (EN → IT) ─────────────────────────────────────────────────
const POS_IT = {
  'Goalkeeper': 'Portiere', 'Defender': 'Difensore', 'Defensor Central': 'Difensore centrale',
  'Left-back': 'Terzino sinistro', 'Right-back': 'Terzino destro',
  'Midfielder': 'Centrocampista', 'Centre-forward': 'Attaccante centrale',
  'Striker': 'Attaccante', 'Forward': 'Attaccante', 'Centre-back': 'Difensore centrale',
};

// ─── Insight generator ──────────────────────────────────────────────────────
function generateInsights(p) {
  const insights = [];
  if (p.tier === 1) insights.push(`Cognome ${p.last_name} è endemico sammarinese — alta probabilità di origine SM`);
  else if (p.tier === 2) insights.push(`Cognome ${p.last_name} ha alta probabilità di origine sammarinese`);
  else insights.push(`Cognome ${p.last_name} è presente tra le famiglie sammarinesi — da verificare`);

  if (p.current_club && p.current_league) insights.push(`Gioca in ${p.current_league} con ${p.current_club}`);
  else if (p.current_club) insights.push(`Tesserato con ${p.current_club}`);
  else insights.push('Attualmente svincolato — potenzialmente disponibile');

  if (p.age) {
    if (p.age <= 23) insights.push(`Giovane (${p.age} anni) — alto potenziale di crescita`);
    else if (p.age <= 28) insights.push(`Età ideale per la nazionale (${p.age} anni)`);
    else insights.push(`Giocatore esperto (${p.age} anni)`);
  }

  const sources = [];
  if (p.cemla_hit) sources.push('CEMLA');
  if (p.ellis_island_hit) sources.push('Ellis Island');
  if (p.familysearch_hit) sources.push('FamilySearch');
  if (sources.length > 0) insights.push(`Confermato in archivi migratori: ${sources.join(', ')}`);

  if (p.position) {
    const posIt = POS_IT[p.position] || p.position;
    insights.push(`Ruolo: ${posIt}`);
  }
  return insights;
}

// ─── Tier / score helpers ───────────────────────────────────────────────────
const tierBadge = (t) => t === 1 ? 'SM' : t === 2 ? 'PR' : 'DA';
const tierBadgeClass = (t) => t === 1 ? 'badge-sm' : t === 2 ? 'badge-pr' : 'badge-da';
const scoreLevel = (s) => s >= 70 ? 'elite' : s >= 50 ? 'high' : s >= 30 ? 'mid' : 'low';
const surnameLabel = (bd) => bd?.W_name >= 45 ? 'Sammarinese confermato' : bd?.W_name >= 30 ? 'Alta probabilità SM' : 'Da verificare';
const verifyLabel = (bd) => {
  const v = bd?.V_osint || 1;
  if (v >= 1.8) return '✓✓✓ Forte';
  if (v >= 1.5) return '✓✓ Buono';
  if (v >= 1.3) return '✓ Trovato';
  return 'In attesa';
};

function App() {
  const [filters, setFilters] = useState({ tier: "all", minScore: 0, cluster: "all", search: "", verifiedOnly: false, hasAge: false });
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [showMap, setShowMap] = useState(true);
  const [showSummary, setShowSummary] = useState(true);
  const cardRefs = useRef({});

  const stats = useMemo(() => {
    const data = playersData || [];
    const countryCounts = data.reduce((acc, p) => { acc[p.birth_country || 'Unknown'] = (acc[p.birth_country || 'Unknown'] || 0) + 1; return acc; }, {});
    return {
      total: data.length,
      elite: data.filter(p => p.titan_score >= 60).length,
      withAge: data.filter(p => p.age != null).length,
      countries: Object.keys(countryCounts).filter(c => c !== 'Unknown').length,
      tier1: data.filter(p => p.tier === 1).length,
      avgScore: data.length ? Math.round(data.reduce((s, p) => s + p.titan_score, 0) / data.length) : 0,
      countryCounts,
    };
  }, []);

  const filteredPlayers = useMemo(() => {
    return playersData
      .filter(p => {
        const name = `${p.first_name} ${p.last_name}`.toLowerCase();
        if (filters.search && !name.includes(filters.search.toLowerCase()) && !(p.current_club || '').toLowerCase().includes(filters.search.toLowerCase())) return false;
        if (filters.tier !== 'all' && p.tier?.toString() !== filters.tier) return false;
        if (p.titan_score < filters.minScore) return false;
        if (filters.cluster !== 'all') {
          const bc = (p.birth_country || '').toLowerCase();
          if (!bc.includes(filters.cluster)) return false;
        }
        if (filters.verifiedOnly && !p.cemla_hit && !p.ellis_island_hit && !p.familysearch_hit && !p.cognomix_hit) return false;
        if (filters.hasAge && p.age == null) return false;
        return true;
      })
      .sort((a, b) => b.titan_score - a.titan_score)
      .slice(0, 150);
  }, [filters]);

  const topPlayer = playersData.length ? [...playersData].sort((a, b) => b.titan_score - a.titan_score)[0] : null;
  const cenciPlayers = playersData.filter(p => p.last_name === 'Cenci');

  const exportCSV = (players) => {
    const headers = ['Punteggio','Nome','Eta','Posizione','Club','Lega','Paese','Classificazione','CEMLA','Ellis','FamilySearch','Cognomix','BDFA URL','Wikidata URL'];
    const rows = players.map(p => [
      p.titan_score, p.full_name || `${p.first_name} ${p.last_name}`, p.age || '', p.position || '',
      p.current_club || '', p.current_league || '', p.birth_country || '',
      p.tier === 1 ? 'Cognome sammarinese' : p.tier === 2 ? 'Alta probabilità' : 'Da verificare',
      p.cemla_hit ? 'Si' : 'No', p.ellis_island_hit ? 'Si' : 'No',
      p.familysearch_hit ? 'Si' : 'No', p.cognomix_hit ? 'Si' : 'No',
      p.bdfa_url || '', p.wikidata_url || ''
    ]);
    const csv = [headers, ...rows].map(r => r.map(c => `"${c}"`).join(',')).join('\n');
    const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `titan_veritas_export_${new Date().toISOString().slice(0,10)}.csv`;
    a.click(); URL.revokeObjectURL(url);
  };

  const copyPlayerCard = (p) => {
    const bd = p.score_breakdown || {};
    const text = [
      `${p.first_name} ${p.last_name} — Punteggio VERITAS: ${p.titan_score}`,
      `Età: ${p.age || 'N/D'} | Posizione: ${POS_IT[p.position] || p.position || 'N/D'}`,
      `Club: ${p.current_club || 'Svincolato'}${p.current_league ? ` (${p.current_league})` : ''}`,
      `Paese: ${p.birth_country || 'N/D'}${p.birth_place ? ` — ${p.birth_place}` : ''}`,
      `Cognome: ${surnameLabel(bd)} | Verifica storica: ${verifyLabel(bd)}`,
      '', '--- Perché è interessante ---',
      ...generateInsights(p).map(i => `• ${i}`),
    ].join('\n');
    navigator.clipboard.writeText(text);
  };

  const scrollToCard = (id) => {
    setSelectedPlayer(null);
    setTimeout(() => {
      const el = cardRefs.current[id];
      if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'center' }); el.classList.add('highlight-pulse'); setTimeout(() => el.classList.remove('highlight-pulse'), 2000); }
    }, 100);
  };

  // ─── Player detail modal ───────────────────────────────────────────────
  const PlayerModal = ({ player, onClose }) => {
    if (!player) return null;
    const bd = player.score_breakdown || {};
    const insights = generateInsights(player);
    return (
      <div className="modal-overlay" onClick={onClose}>
        <div className="modal-content" onClick={e => e.stopPropagation()}>
          <button className="modal-close" onClick={onClose}>&#8592; Chiudi</button>

          <div className="modal-hero">
            <div className="modal-score" data-level={scoreLevel(player.titan_score)}>{player.titan_score}</div>
            <div className="modal-title">
              <h2>{player.first_name} {player.last_name}</h2>
              <p className="modal-subtitle">
                {POS_IT[player.position] || player.position || 'N/D'}
                {player.current_club && <> &bull; {player.current_club}</>}
                {player.current_league && <> &bull; {player.current_league}</>}
              </p>
              <p className="modal-subtitle">
                {player.birth_country || 'N/D'}
                {player.age && <> &bull; {player.age} anni</>}
              </p>
            </div>
          </div>

          <div className="modal-badges-row">
            <div className="modal-badge-card">
              <span className="modal-badge-label">Cognome</span>
              <span className="modal-badge-value">{surnameLabel(bd)}</span>
              <span className="modal-badge-check">{bd.W_name >= 45 ? '✓' : '~'}</span>
            </div>
            <div className="modal-badge-card">
              <span className="modal-badge-label">Diaspora</span>
              <span className="modal-badge-value">{player.birth_country || 'N/D'}</span>
              <span className="modal-badge-check">{bd.W_geo > 0 ? '✓' : '—'}</span>
            </div>
            <div className="modal-badge-card">
              <span className="modal-badge-label">Verifica</span>
              <span className="modal-badge-value">{verifyLabel(bd)}</span>
              <span className="modal-badge-check">{bd.V_osint >= 1.5 ? '✓✓' : bd.V_osint >= 1.3 ? '✓' : '—'}</span>
            </div>
          </div>

          <div className="modal-section">
            <h4>Profilo</h4>
            <div className="modal-profile-grid">
              <div className="profile-row"><span>Nome completo</span><span>{player.first_name} {player.last_name}</span></div>
              <div className="profile-row"><span>Data di nascita</span><span>{player.date_of_birth || '—'}</span></div>
              <div className="profile-row"><span>Luogo di nascita</span><span>{[player.birth_place, player.birth_country].filter(Boolean).join(', ') || '—'}</span></div>
              <div className="profile-row"><span>Posizione</span><span>{POS_IT[player.position] || player.position || '—'}</span></div>
              <div className="profile-row"><span>Club attuale</span><span>{player.current_club || 'Svincolato'}{player.current_league ? ` (${player.current_league})` : ''}</span></div>
            </div>
          </div>

          <div className="modal-section insights-section">
            <h4>Perché è interessante</h4>
            <ul className="insights-list">
              {insights.map((ins, i) => <li key={i}>{ins}</li>)}
            </ul>
          </div>

          <div className="modal-section">
            <h4>Fonti</h4>
            <div className="modal-links">
              {player.wikidata_url && <a href={player.wikidata_url} target="_blank" rel="noopener noreferrer" className="profile-link">Wikidata &#8599;</a>}
              {player.bdfa_url && <a href={player.bdfa_url} target="_blank" rel="noopener noreferrer" className="profile-link bdfa-link">BDFA &#8599;</a>}
            </div>
          </div>

          <button className="copy-btn" onClick={() => copyPlayerCard(player)}>Copia scheda negli appunti</button>
        </div>
      </div>
    );
  };

  // ─── Render ────────────────────────────────────────────────────────────
  return (
    <div className="hud-container">
      <header>
        <div className="logo-group">
          <h1>TITAN VERITAS <span className="v-tag">v6.4</span></h1>
          <span className="subtitle">Identificazione Oriundi Sammarinesi</span>
        </div>
      </header>

      {/* ── STEP 4: Executive Summary ─────────────────────────────────── */}
      <div className={`exec-summary ${showSummary ? '' : 'collapsed'}`}>
        <div className="exec-header" onClick={() => setShowSummary(!showSummary)}>
          <span className="exec-title-text">Report Esecutivo</span>
          <span className="exec-toggle">{showSummary ? '▲' : '▼'}</span>
        </div>
        {showSummary && (
          <div className="exec-body">
            <p className="exec-intro">
              Sistema di identificazione calciatori oriundi sammarinesi nella diaspora,
              eleggibili per <em>jus sanguinis</em>. Analizzati <strong>2.149</strong> profili,
              identificati <strong>{stats.total}</strong> candidati verificati in <strong>{stats.countries}</strong> paesi.
            </p>
            <div className="exec-highlights">
              <div className="exec-card" onClick={() => topPlayer && scrollToCard(topPlayer.id)}>
                <span className="exec-card-label">Top Candidato</span>
                <span className="exec-card-value">{topPlayer ? `${topPlayer.first_name.charAt(0)}. ${topPlayer.last_name}` : '—'}</span>
                <span className="exec-card-detail">{topPlayer?.titan_score} pts &bull; {topPlayer?.current_club || 'Svincolato'}</span>
              </div>
              <div className="exec-card" onClick={() => setFilters({...filters, search: 'Cenci'})}>
                <span className="exec-card-label">Scoperta</span>
                <span className="exec-card-value">Fratelli Cenci</span>
                <span className="exec-card-detail">Ex vivaio Boca Juniors ({cenciPlayers.length} giocatori)</span>
              </div>
              <div className="exec-card" onClick={() => setShowMap(true)}>
                <span className="exec-card-label">Copertura</span>
                <span className="exec-card-value">{stats.countries} paesi</span>
                <span className="exec-card-detail">{Object.entries(stats.countryCounts).filter(([k]) => k !== 'Unknown').map(([k,v]) => `${COUNTRY_LABELS_IT[k] || k} (${v})`).join(', ')}</span>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* ── KPI Stats ─────────────────────────────────────────────────── */}
      <div className="stats-grid">
        <div className="stat-card"><span className="stat-value">{stats.total}</span><span className="stat-label">Oriundi Identificati</span></div>
        <div className="stat-card accent"><span className="stat-value">{stats.elite}</span><span className="stat-label">Priorità Alta</span></div>
        <div className="stat-card"><span className="stat-value">{stats.tier1}</span><span className="stat-label">Cognome Endemico</span></div>
        <div className="stat-card"><span className="stat-value">{stats.countries}</span><span className="stat-label">Paesi</span></div>
        <div className="stat-card"><span className="stat-value">{stats.withAge}</span><span className="stat-label">Età Confermata</span></div>
        <div className="stat-card"><span className="stat-value">{stats.avgScore}</span><span className="stat-label">Punteggio Medio</span></div>
      </div>

      {/* ── Filters ───────────────────────────────────────────────────── */}
      <div className="filters-bar">
        <input type="text" placeholder="Cerca nome, cognome o club..." className="search-input"
          value={filters.search} onChange={(e) => setFilters({...filters, search: e.target.value})} />
        <select className="filter-select" value={filters.tier} onChange={(e) => setFilters({...filters, tier: e.target.value})}>
          <option value="all">Tutti i cognomi</option>
          <option value="1">Cognome sammarinese</option>
          <option value="2">Alta probabilità</option>
          <option value="3">Da verificare</option>
        </select>
        <select className="filter-select" value={filters.cluster} onChange={(e) => setFilters({...filters, cluster: e.target.value})}>
          <option value="all">Tutti i paesi</option>
          <option value="argentin">Argentina</option>
          <option value="france">Francia</option>
          <option value="chile">Cile</option>
        </select>
        <div className="score-slider">
          <label>Min Punteggio: <strong>{filters.minScore}</strong></label>
          <input type="range" min="0" max="200" value={filters.minScore} onChange={(e) => setFilters({...filters, minScore: parseInt(e.target.value)})} />
        </div>
        <label className="toggle-label">
          <input type="checkbox" checked={filters.hasAge} onChange={(e) => setFilters({...filters, hasAge: e.target.checked})} />
          Solo con età
        </label>
        <label className="toggle-label">
          <input type="checkbox" checked={filters.verifiedOnly} onChange={(e) => setFilters({...filters, verifiedOnly: e.target.checked})} />
          Solo con verifica storica
        </label>
      </div>

      {/* ── STEP 2: Geographic Distribution ──────────────────────────── */}
      <div className="map-section">
        <div className="map-header" onClick={() => setShowMap(!showMap)}>
          <span>Distribuzione Geografica</span>
          <span className="map-toggle">{showMap ? '▲ Nascondi' : '▼ Mostra'}</span>
        </div>
        {showMap && (
          <div className="geo-chart">
            {Object.entries(stats.countryCounts)
              .filter(([k]) => k !== 'Unknown')
              .sort(([,a], [,b]) => b - a)
              .map(([country, count]) => {
                const pct = Math.round((count / stats.total) * 100);
                const flag = country === 'Argentina' ? '🇦🇷' : country === 'France' ? '🇫🇷' : country === 'Chile' ? '🇨🇱' : '🌍';
                const countryPlayers = filteredPlayers.filter(p => (p.birth_country || '') === country);
                return (
                  <div key={country} className="geo-bar-row">
                    <span className="geo-flag">{flag}</span>
                    <span className="geo-country">{COUNTRY_LABELS_IT[country] || country}</span>
                    <div className="geo-bar-track">
                      <div className="geo-bar-fill" style={{ width: `${pct}%` }}></div>
                    </div>
                    <span className="geo-count">{count} ({pct}%)</span>
                    <div className="geo-players-mini">
                      {countryPlayers.slice(0, 3).map(p => (
                        <span key={p.id} className="geo-player-chip" onClick={() => setSelectedPlayer(p)}>
                          {p.last_name}
                        </span>
                      ))}
                      {countryPlayers.length > 3 && <span className="geo-more">+{countryPlayers.length - 3}</span>}
                    </div>
                  </div>
                );
              })}
          </div>
        )}
      </div>

      {/* ── Results count + export ─────────────────────────────────────── */}
      <div className="results-count">
        <span>{filteredPlayers.length} oriundi {filteredPlayers.length === 150 ? '(primi 150)' : ''}</span>
        <button onClick={() => exportCSV(filteredPlayers)} className="export-btn">Esporta CSV</button>
      </div>

      {/* ── Player Cards Grid ─────────────────────────────────────────── */}
      <div className="players-grid">
        {filteredPlayers.map((p) => {
          const bd = p.score_breakdown || {};
          return (
            <div key={p.id} ref={el => cardRefs.current[p.id] = el}
              className={`player-card tier-${p.tier}`}
              onClick={() => setSelectedPlayer(p)}>
              <div className="card-header">
                <div className="score-badge" data-level={scoreLevel(p.titan_score)}>{p.titan_score}</div>
                <div className="card-title">
                  <h3 className="player-name">{p.full_name || `${p.first_name} ${p.last_name}`}</h3>
                  <div className="player-meta">
                    {p.age != null && <span className="meta-tag age">{p.age} anni</span>}
                    <span className={`meta-tag ${tierBadgeClass(p.tier)}`}>{tierBadge(p.tier)}</span>
                    {(p.cemla_hit || p.ellis_island_hit || p.familysearch_hit) && <span className="meta-tag verified-tag">Verificato</span>}
                  </div>
                </div>
              </div>
              <div className="card-details">
                {(p.birth_place || p.birth_country) && (
                  <div className="detail-row"><span className="detail-icon">&#128205;</span>{[p.birth_place, p.birth_country].filter(Boolean).join(', ')}</div>
                )}
                {p.current_club && (
                  <div className="detail-row"><span className="detail-icon">&#9917;</span>{p.current_club}{p.current_league && <span className="league-tag"> ({p.current_league})</span>}</div>
                )}
                {p.position && (
                  <div className="detail-row"><span className="detail-icon">&#127919;</span>{POS_IT[p.position] || p.position}</div>
                )}
              </div>

              {Object.keys(bd).length > 0 && (
                <div className="breakdown">
                  <div className="breakdown-title">Analisi Candidato</div>
                  <div className="formula-grid">
                    <div className="formula-section">
                      <span className="formula-label">Cognome</span>
                      <span className="formula-val">{surnameLabel(bd)}</span>
                    </div>
                    <div className="formula-section">
                      <span className="formula-label">Diaspora</span>
                      <span className="formula-val">{bd.W_geo > 0 ? '✓ Paese confermato' : 'Non disponibile'}</span>
                    </div>
                    <div className="formula-section">
                      <span className="formula-label">Club</span>
                      <span className="formula-val">{bd.M_athletic > 0 ? '✓ Verificato' : 'Non disponibile'}</span>
                    </div>
                    <div className="formula-section">
                      <span className="formula-label">Verifica storica</span>
                      <span className="formula-val">{verifyLabel(bd)}</span>
                    </div>
                    <div className="formula-section">
                      <span className="formula-label">Età</span>
                      <span className="formula-val">{bd.age_method === 'exact' ? '✓ Confermata' : bd.age_method === 'career_proxy' ? '~ Stimata' : 'Non nota'}</span>
                    </div>
                    <div className="formula-divider"></div>
                    <div className="formula-section formula-total">
                      <span className="formula-label">Punteggio VERITAS</span>
                      <span className="formula-val">{bd.S_total ?? p.titan_score}</span>
                    </div>
                  </div>
                </div>
              )}

              <div className="card-links">
                {p.wikidata_url && <a href={p.wikidata_url} target="_blank" rel="noopener noreferrer" className="profile-link" onClick={e => e.stopPropagation()}>Wikidata &#8599;</a>}
                {p.bdfa_url && <a href={p.bdfa_url} target="_blank" rel="noopener noreferrer" className="profile-link bdfa-link" onClick={e => e.stopPropagation()}>BDFA &#8599;</a>}
              </div>
            </div>
          );
        })}
      </div>

      <footer className="hud-footer">
        TITAN VERITAS v6.4 &mdash; Federazione Sammarinese Giuoco Calcio
      </footer>

      {/* ── STEP 3: Player Modal ──────────────────────────────────────── */}
      {selectedPlayer && <PlayerModal player={selectedPlayer} onClose={() => setSelectedPlayer(null)} />}
    </div>
  );
}

export default App;
