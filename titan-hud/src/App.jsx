import React, { useState, useMemo } from 'react';
import playersData from './data.json';

function App() {
  const [filters, setFilters] = useState({
    tier: "all",
    minScore: 0,
    cluster: "all",
    search: "",
    osintOnly: false,
    hasAge: false,
  });
  const [expandedId, setExpandedId] = useState(null);

  const stats = useMemo(() => {
    const data = playersData || [];
    const countryCounts = data.reduce((acc, p) => {
      const c = p.birth_country || 'Unknown';
      acc[c] = (acc[c] || 0) + 1;
      return acc;
    }, {});
    return {
      total: data.length,
      elite: data.filter(p => p.titan_score >= 60).length,
      withAge: data.filter(p => p.age != null).length,
      withClub: data.filter(p => p.current_club).length,
      countries: Object.keys(countryCounts).length,
      tier1: data.filter(p => p.tier === 1).length,
      avgScore: data.length ? Math.round(data.reduce((s, p) => s + p.titan_score, 0) / data.length) : 0,
    };
  }, []);

  const filteredPlayers = useMemo(() => {
    return playersData
      .filter(p => {
        const name = `${p.first_name} ${p.last_name}`.toLowerCase();
        if (filters.search && !name.includes(filters.search.toLowerCase()) &&
            !(p.current_club || '').toLowerCase().includes(filters.search.toLowerCase())) return false;
        if (filters.tier !== 'all' && p.tier?.toString() !== filters.tier) return false;
        if (p.titan_score < filters.minScore) return false;
        if (filters.cluster !== 'all') {
          const bc = (p.birth_country || '').toLowerCase();
          const nats = (p.nationalities || []).join(' ').toLowerCase();
          if (!bc.includes(filters.cluster) && !nats.includes(filters.cluster)) return false;
        }
        if (filters.osintOnly && !p.cemla_hit && !p.ellis_island_hit && !p.familysearch_hit && !p.cognomix_hit) return false;
        if (filters.hasAge && p.age == null) return false;
        return true;
      })
      .sort((a, b) => b.titan_score - a.titan_score)
      .slice(0, 150);
  }, [filters]);

  const scoreLevel = (s) => s >= 70 ? 'elite' : s >= 50 ? 'high' : s >= 30 ? 'mid' : 'low';

  const exportCSV = (players) => {
    const headers = ['Score','Nome','Eta','Posizione','Club','Lega','Paese','Tier','CEMLA','Ellis','FamilySearch','Cognomix','BDFA URL','Wikidata URL'];
    const rows = players.map(p => [
      p.titan_score,
      p.full_name || `${p.first_name} ${p.last_name}`,
      p.age || '',
      p.position || '',
      p.current_club || '',
      p.current_league || '',
      p.birth_country || '',
      `Tier ${p.tier}`,
      p.cemla_hit ? 'Si' : 'No',
      p.ellis_island_hit ? 'Si' : 'No',
      p.familysearch_hit ? 'Si' : 'No',
      p.cognomix_hit ? 'Si' : 'No',
      p.bdfa_url || '',
      p.wikidata_url || ''
    ]);
    const csv = [headers, ...rows].map(r => r.map(c => `"${c}"`).join(',')).join('\n');
    const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `titan_veritas_export_${new Date().toISOString().slice(0,10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="hud-container">
      <header>
        <div className="logo-group">
          <h1>TITAN VERITAS <span className="v-tag">v6.3</span></h1>
          <span className="subtitle">San Marino Diaspora Football Intelligence</span>
        </div>
      </header>

      <div className="stats-grid">
        <div className="stat-card">
          <span className="stat-value">{stats.total.toLocaleString()}</span>
          <span className="stat-label">Candidati Totali</span>
        </div>
        <div className="stat-card accent">
          <span className="stat-value">{stats.elite}</span>
          <span className="stat-label">Elite (Score 60+)</span>
        </div>
        <div className="stat-card">
          <span className="stat-value">{stats.tier1}</span>
          <span className="stat-label">Tier 1 (Cognome Raro)</span>
        </div>
        <div className="stat-card">
          <span className="stat-value">{stats.countries}</span>
          <span className="stat-label">Nazioni</span>
        </div>
        <div className="stat-card">
          <span className="stat-value">{stats.withAge}</span>
          <span className="stat-label">Con Età Nota</span>
        </div>
        <div className="stat-card">
          <span className="stat-value">{stats.avgScore}</span>
          <span className="stat-label">Score Medio</span>
        </div>
      </div>

      <div className="filters-bar">
        <input
          type="text"
          placeholder="Cerca nome, cognome o club..."
          className="search-input"
          value={filters.search}
          onChange={(e) => setFilters({...filters, search: e.target.value})}
        />
        <select className="filter-select" value={filters.tier}
          onChange={(e) => setFilters({...filters, tier: e.target.value})}>
          <option value="all">Tutti i Tier</option>
          <option value="1">Tier 1 (Endemici)</option>
          <option value="2">Tier 2 (Alta Prob.)</option>
          <option value="3">Tier 3 (Varianti)</option>
        </select>
        <select className="filter-select" value={filters.cluster}
          onChange={(e) => setFilters({...filters, cluster: e.target.value})}>
          <option value="all">Tutte le Nazioni</option>
          <option value="argentin">Argentina</option>
          <option value="france">Francia</option>
          <option value="brazil">Brasile</option>
          <option value="united states">USA</option>
          <option value="belgium">Belgio</option>
        </select>
        <div className="score-slider">
          <label>Min Score: <strong>{filters.minScore}</strong></label>
          <input type="range" min="0" max="100" value={filters.minScore}
            onChange={(e) => setFilters({...filters, minScore: parseInt(e.target.value)})} />
        </div>
        <label className="toggle-label">
          <input type="checkbox" checked={filters.hasAge}
            onChange={(e) => setFilters({...filters, hasAge: e.target.checked})} />
          Solo con età
        </label>
        <label className="toggle-label">
          <input type="checkbox" checked={filters.osintOnly}
            onChange={(e) => setFilters({...filters, osintOnly: e.target.checked})} />
          Solo OSINT+
        </label>
      </div>

      <div className="results-count">
        <span>{filteredPlayers.length} risultati {filteredPlayers.length === 150 ? '(top 150)' : ''}</span>
        <button onClick={() => exportCSV(filteredPlayers)} className="export-btn">
          Esporta CSV
        </button>
      </div>

      <div className="players-grid">
        {filteredPlayers.map((p) => {
          const bd = p.score_breakdown || {};
          const isExpanded = expandedId === p.id;
          return (
            <div key={p.id} className={`player-card tier-${p.tier}`}
                 onClick={() => setExpandedId(isExpanded ? null : p.id)}>
              <div className="card-header">
                <div className="score-badge" data-level={scoreLevel(p.titan_score)}>
                  {p.titan_score}
                </div>
                <div className="card-title">
                  <h3 className="player-name">{p.full_name || `${p.first_name} ${p.last_name}`}</h3>
                  <div className="player-meta">
                    {p.age != null && <span className="meta-tag age">Età {p.age}</span>}
                    <span className="meta-tag tier-tag">T{p.tier}</span>
                    {(p.sources || []).map((s, i) => (
                      <span key={i} className="meta-tag source-tag">{s}</span>
                    ))}
                    {p.cemla_hit && <span className="meta-tag osint-tag">CEMLA</span>}
                    {p.ellis_island_hit && <span className="meta-tag osint-tag">ELLIS</span>}
                    {p.familysearch_hit && <span className="meta-tag osint-tag">FS</span>}
                    {p.cognomix_hit && <span className="meta-tag osint-tag">CGX</span>}
                  </div>
                </div>
              </div>
              <div className="card-details">
                {p.nationalities?.length > 0 && (
                  <div className="detail-row">
                    <span className="detail-icon">&#127758;</span>
                    {p.nationalities.join(', ')}
                  </div>
                )}
                {(p.birth_place || p.birth_country) && (
                  <div className="detail-row">
                    <span className="detail-icon">&#128205;</span>
                    {[p.birth_place, p.birth_country].filter(Boolean).join(', ')}
                  </div>
                )}
                {p.current_club && (
                  <div className="detail-row">
                    <span className="detail-icon">&#9917;</span>
                    {p.current_club}
                    {p.current_league && <span className="league-tag"> ({p.current_league})</span>}
                  </div>
                )}
                {p.position && (
                  <div className="detail-row">
                    <span className="detail-icon">&#127919;</span>
                    {p.position}
                  </div>
                )}
              </div>

              {Object.keys(bd).length > 0 && (
                <div className="breakdown">
                  <div className="breakdown-title">Analisi Candidato</div>
                  <div className="formula-grid">
                    <div className="formula-section">
                      <span className="formula-label">Cognome</span>
                      <span className="formula-val">{bd.W_name >= 45 ? 'Endemico' : bd.W_name >= 30 ? 'Probabile' : 'Generico'}</span>
                    </div>
                    <div className="formula-section">
                      <span className="formula-label">Paese diaspora</span>
                      <span className="formula-val">{bd.W_geo > 0 ? '✓ Confermato' : '—'}</span>
                    </div>
                    <div className="formula-section">
                      <span className="formula-label">Club verificato</span>
                      <span className="formula-val">{bd.M_athletic > 0 ? '✓ Sì' : '—'}</span>
                    </div>
                    <div className="formula-section">
                      <span className="formula-label">OSINT verifiche</span>
                      <span className="formula-val">{bd.V_osint >= 1.8 ? '✓✓✓ Forte' : bd.V_osint >= 1.5 ? '✓✓ Buono' : bd.V_osint >= 1.3 ? '✓ Parziale' : 'In attesa'}</span>
                    </div>
                    <div className="formula-section">
                      <span className="formula-label">Età</span>
                      <span className="formula-val">{bd.age_method === 'exact' ? 'Verificata' : bd.age_method === 'career_proxy' ? 'Stimata' : 'Non nota'}</span>
                    </div>
                    <div className="formula-divider"></div>
                    <div className="formula-section formula-total">
                      <span className="formula-label">Punteggio TITAN</span>
                      <span className="formula-val">{bd.S_total ?? p.titan_score}</span>
                    </div>
                  </div>
                </div>
              )}

              <div className="card-links">
                {p.wikidata_url && (
                  <a href={p.wikidata_url} target="_blank" rel="noopener noreferrer" className="profile-link"
                     onClick={(e) => e.stopPropagation()}>Wikidata &#8599;</a>
                )}
                {p.bdfa_url && (
                  <a href={p.bdfa_url} target="_blank" rel="noopener noreferrer" className="profile-link bdfa-link"
                     onClick={(e) => e.stopPropagation()}>BDFA &#8599;</a>
                )}
              </div>
              {isExpanded && p.date_of_birth && (
                <div className="expanded-details">
                  <div className="exp-row">Data nascita: {p.date_of_birth}</div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      <footer className="hud-footer">
        TITAN VERITAS v6.3 &mdash; Powered by Wikidata + BDFA + API-Football + CEMLA + Ellis Island
      </footer>
    </div>
  );
}

export default App;
