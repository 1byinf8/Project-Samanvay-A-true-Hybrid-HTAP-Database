import { useState, useEffect, useRef, useCallback } from 'react';
import { executeQuery, checkHealth } from './api';
import './App.css';

function App() {
  const [sql, setSql] = useState('');
  const [results, setResults] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isConnected, setIsConnected] = useState(false);
  const [history, setHistory] = useState([]);
  const textareaRef = useRef(null);
  const resultsEndRef = useRef(null);

  // Health check on mount + interval
  useEffect(() => {
    const check = async () => {
      try {
        const res = await checkHealth();
        setIsConnected(res.success === true);
      } catch {
        setIsConnected(false);
      }
    };
    check();
    const interval = setInterval(check, 5000);
    return () => clearInterval(interval);
  }, []);

  // Auto-scroll to latest result
  useEffect(() => {
    if (resultsEndRef.current) {
      resultsEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [results]);

  const runQuery = useCallback(async () => {
    const trimmed = sql.trim();
    if (!trimmed || isLoading) return;

    setIsLoading(true);
    const startTime = performance.now();

    try {
      const response = await executeQuery(trimmed);
      const elapsed = Math.round(performance.now() - startTime);

      const entry = {
        id: Date.now(),
        sql: trimmed,
        response,
        elapsed,
        timestamp: new Date().toLocaleTimeString(),
      };

      setResults(prev => [...prev, entry]);
      setHistory(prev => [entry, ...prev].slice(0, 50));
      setSql('');
    } catch (err) {
      const entry = {
        id: Date.now(),
        sql: trimmed,
        response: {
          success: false,
          error: { message: `Connection error: ${err.message}`, type: 'NETWORK_ERROR' },
        },
        elapsed: 0,
        timestamp: new Date().toLocaleTimeString(),
      };
      setResults(prev => [...prev, entry]);
      setHistory(prev => [entry, ...prev].slice(0, 50));
    } finally {
      setIsLoading(false);
      textareaRef.current?.focus();
    }
  }, [sql, isLoading]);

  // Keyboard shortcuts
  const handleKeyDown = useCallback((e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      runQuery();
    }
  }, [runQuery]);

  const clearResults = () => setResults([]);

  const loadFromHistory = (item) => {
    setSql(item.sql);
    textareaRef.current?.focus();
  };

  return (
    <>
      {/* ═══ HEADER ═══ */}
      <header className="main-header">
        <div className="header-left">
          <span className="logo-icon">⚡</span>
          <div className="logo-text">
            <h1>Samanvay</h1>
            <span className="subtitle">SQL Console</span>
          </div>
        </div>
        <div className="header-right">
          <div className={`connection-status ${isConnected ? 'connected' : 'disconnected'}`}>
            <span className="status-dot"></span>
            {isConnected ? 'Connected' : 'Disconnected'}
          </div>
        </div>
      </header>

      {/* ═══ MAIN CONTENT ═══ */}
      <div className="app-container">
        {/* SQL Input */}
        <div className="sql-input-panel">
          <div className="panel-header">
            <span className="panel-icon">⌨️</span>
            <h2>SQL Query</h2>
            <span className="panel-badge">{history.length} queries</span>
          </div>
          <div className="sql-editor-wrapper">
            <textarea
              ref={textareaRef}
              className="sql-textarea"
              value={sql}
              onChange={(e) => setSql(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Enter SQL statement... (e.g. CREATE TABLE, INSERT, SELECT)"
              spellCheck={false}
              autoFocus
            />
            <div className="sql-toolbar">
              <div className="toolbar-left">
                <button
                  className="btn btn-primary"
                  onClick={runQuery}
                  disabled={!sql.trim() || isLoading}
                >
                  {isLoading ? (
                    <><span className="loading-spinner"></span> Running...</>
                  ) : (
                    <><span className="btn-icon">▶</span> Execute</>
                  )}
                </button>
                <button className="btn btn-danger" onClick={clearResults}>
                  <span className="btn-icon">🗑</span> Clear
                </button>
              </div>
              <div className="toolbar-right">
                <span className="shortcut-hint">⌘+Enter to run</span>
              </div>
            </div>
          </div>
        </div>

        {/* Results */}
        <div className="results-panel">
          <div className="panel-header">
            <span className="panel-icon">📊</span>
            <h2>Results</h2>
            {results.length > 0 && (
              <span className="panel-badge">{results.length} results</span>
            )}
          </div>
          <div className="results-content">
            {results.length === 0 ? (
              <div className="empty-state">
                <span className="empty-state-icon">💡</span>
                <div className="empty-state-text">
                  Run a SQL query to see results here
                </div>
                <div className="empty-state-hint">
                  Try: SELECT * FROM your_table
                </div>
              </div>
            ) : (
              <div className="results-list">
                {results.map((entry) => (
                  <ResultEntry key={entry.id} entry={entry} />
                ))}
                <div ref={resultsEndRef} />
              </div>
            )}
          </div>
        </div>

        {/* History Sidebar */}
        {history.length > 0 && (
          <div className="history-panel">
            <div className="panel-header">
              <span className="panel-icon">🕐</span>
              <h2>History</h2>
            </div>
            <div className="history-list">
              {history.map((item) => (
                <div
                  key={item.id}
                  className={`history-item ${!item.response.success ? 'error' : ''}`}
                  onClick={() => loadFromHistory(item)}
                >
                  <span className={`history-icon ${item.response.success ? 'success' : 'error'}`}>
                    {item.response.success ? '✓' : '✗'}
                  </span>
                  <span className="history-sql">{item.sql}</span>
                  <span className="history-time">{item.timestamp}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </>
  );
}

// ═══ RESULT ENTRY COMPONENT ═══
function ResultEntry({ entry }) {
  const { sql, response, elapsed } = entry;

  return (
    <div className="result-entry">
      {/* Query header */}
      <div className="result-query-bar">
        <span className="result-query-icon">›</span>
        <code className="result-query-sql">{sql}</code>
        <span className="result-query-time">{elapsed}ms</span>
      </div>

      {/* Result body */}
      {!response.success ? (
        <div className="result-message error">
          <span className="result-message-icon">✗</span>
          <div className="result-message-text">
            <strong>{response.error?.type || 'ERROR'}:</strong>{' '}
            {response.error?.message || 'Unknown error'}
          </div>
        </div>
      ) : response.data?.headers?.length > 0 ? (
        <div className="results-table-container">
          <table className="results-table">
            <thead>
              <tr>
                {response.data.headers.map((h, i) => (
                  <th key={i}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {response.data.rows.length === 0 ? (
                <tr>
                  <td colSpan={response.data.headers.length} style={{ textAlign: 'center', color: 'var(--text-muted)' }}>
                    No rows returned
                  </td>
                </tr>
              ) : (
                response.data.rows.map((row, ri) => (
                  <tr key={ri}>
                    {row.map((cell, ci) => (
                      <td key={ci}>{cell}</td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
          <div className="result-footer">
            {response.data.rows.length} row{response.data.rows.length !== 1 ? 's' : ''} returned
          </div>
        </div>
      ) : (
        <div className="result-message success">
          <span className="result-message-icon">✓</span>
          <div className="result-message-text">
            {response.data?.message || `OK — ${response.data?.rowsAffected || 0} row(s) affected`}
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
