import React, { useState, useMemo } from 'react';
import playersData from './data.json';

function App() {
  const [searchTerm, setSearchTerm] = useState('');
  const [minScore, setMinScore] = useState(0);
  const [tierFilter, setTierFilter] = useState('all');

  const stats = useMemo(() => {
    const countryCounts = (playersData || []).reduce((acc, p) => {
      const country = p.birth_country || 'Unknown';
      acc[country] = (acc[country] || 0) + 1;
      return acc;
    }, {});
    
    const topCountries = Object.entries(countryCounts)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 3)
      .map(([name, count]) => `${name} (${count})`)
      .join(', ');

    return {
      total: playersData.length,
      topConfidence: playersData.filter(p => (p.score || 0) >= 40).length,
      countries: Object.keys(countryCounts).length,
      topClusters: topCountries || 'Nessun cluster'
    };
  }, []);

  const filteredPlayers = useMemo(() => {
    return playersData
      .filter(p => {
        const name = (p.name || '').toLowerCase();
        const lastName = (p.last_name || '').toLowerCase();
        const search = searchTerm.toLowerCase();

        const matchesSearch = name.includes(search) || lastName.includes(search);
        const matchesScore = (p.score || 0) >= minScore;
        const matchesTier = tierFilter === 'all' || (p.tier && p.tier.toString() === tierFilter);
        return matchesSearch && matchesScore && matchesTier;
      })
      .sort((a, b) => (b.score || 0) - (a.score || 0))
      .slice(0, 50); // Mostriamo solo i primi 50 per performance
  }, [searchTerm, minScore, tierFilter]);旋

  return (
    <div className="hud-container">
      <header>
        <div>
          <h1>TITAN VERITAS <span style={{fontSize: '0.4em', verticalAlign: 'middle', opacity: 0.6}}>v5.0</span></h1>
          <p style={{color: '#94a3b8', margin: '5px 0'}}>Strategic Scouting Intelligence - San Marino</p>
        </div>
        <div style={{textAlign: 'right'}}>
          <span className="source-tag" style={{fontSize: '0.9rem'}}>Agent Mode: ACTIVE SCOUTING</span>
        </div>
      </header>

      <div className="stats-grid">
        <div className="stat-card">
          <span className="stat-value">{stats.total.toLocaleString()}</span>
          <span className="stat-label">Sospetti Totali</span>
        </div>
        <div className="stat-card">
          <span className="stat-value">{stats.topConfidence}</span>
          <span className="stat-label">Alta Confidenza (Score 40+)</span>
        </div>
        <div className="stat-card">
          <span className="stat-value">{stats.countries}</span>
          <span className="stat-label">Nazioni Schedatete</span>
        </div>
        <div className="stat-card" style={{gridColumn: 'span 2'}}>
          <span className="stat-value" style={{fontSize: '1.2rem', color: '#C8A951'}}>{stats.topClusters}</span>
          <span className="stat-label">Top Cluster Geografici</span>
        </div>
      </div>

      <div className="filters-bar">
        <input 
          type="text" 
          placeholder="Cerca per nome o cognome..." 
          className="search-input"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
        />
        
        <select 
          className="filter-select"
          value={tierFilter}
          onChange={(e) => setTierFilter(e.target.value)}
        >
          <option value="all">Tutti i Tier</option>
          <option value="1">Tier 1 (Rari)</option>
          <option value="2">Tier 2 (Diffusi)</option>
          <option value="3">Tier 3 (Fuzzy/Materni)</option>
        </select>

        <div style={{display: 'flex', alignItems: 'center', gap: '10px'}}>
          <label style={{fontSize: '0.8rem', color: '#94a3b8'}}>Min Score:</label>
          <input 
            type="range" 
            min="0" 
            max="50" 
            value={minScore} 
            onChange={(e) => setMinScore(parseInt(e.target.value))}
          />
          <span style={{color: '#66B2FF', fontWeight: 'bold'}}>{minScore}</span>
        </div>
      </div>

      <div className="players-grid">
        {filteredPlayers.map((player, idx) => (
          <div key={idx} className={`player-card ${player.tier === 1 ? 'tier-1' : ''}`}>
            <div className="score-badge">{player.score}</div>
            <h3 style={{margin: '0 0 10px 0'}}>{player.name}</h3>
            
            <div style={{fontSize: '0.9rem', color: '#94a3b8'}}>
              <div><strong>NAZIONE:</strong> {player.birth_country}</div>
              <div><strong>CLUB:</strong> {player.current_club || 'N/A'}</div>
            </div>

            <div className="source-tag">Sorgente: {player.source}</div>

            <div className="breakdown">
              <strong>DETTAGLI SCOUTING:</strong>
              <ul>
                {player.breakdown.map((item, i) => (
                  <li key={i}>{item}</li>
                ))}
              </ul>
            </div>

            <a href={player.source_url} target="_blank" rel="noreferrer" className="btn-profile">
              Analizza Profilo
            </a>
          </div>
        ))}
      </div>

      <div className="virtual-info">
        Sto visualizzando i migliori 50 risultati su {playersData.length.toLocaleString()}. 
        Usa i filtri per raffinare la ricerca.
      </div>
    </div>
  );
}

export default App;
