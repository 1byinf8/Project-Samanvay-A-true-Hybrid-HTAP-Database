import { useState, useEffect, useRef, useCallback } from 'react';
import { executeQuery, checkHealth, listTables, getTableSchema, getStatus, getInfo } from './api';

// ═══════════════════════════════════════════════════════════════
// SVG ICON COMPONENTS — no emojis, clean professional icons
// ═══════════════════════════════════════════════════════════════
const Icon = ({ children, size = 14, className = '' }) => (
  <span className={`icon ${className}`} style={{ fontSize: size }}>
    {children}
  </span>
);

const IconTerminal = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><polyline points="4 17 10 11 4 5" /><line x1="12" y1="19" x2="20" y2="19" /></svg></Icon>;
const IconPlay = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><polygon points="5 3 19 12 5 21 5 3" /></svg></Icon>;
const IconTrash = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6" /><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /></svg></Icon>;
const IconDatabase = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><ellipse cx="12" cy="5" rx="9" ry="3" /><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" /><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" /></svg></Icon>;
const IconTable = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2" /><line x1="3" y1="9" x2="21" y2="9" /><line x1="3" y1="15" x2="21" y2="15" /><line x1="9" y1="3" x2="9" y2="21" /></svg></Icon>;
const IconColumns = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="1.5" /></svg></Icon>;
const IconChevron = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><polyline points="9 18 15 12 9 6" /></svg></Icon>;
const IconGrid = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" /><rect x="14" y="3" width="7" height="7" /><rect x="3" y="14" width="7" height="7" /><rect x="14" y="14" width="7" height="7" /></svg></Icon>;
const IconMessageSquare = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" /></svg></Icon>;
const IconBarChart = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><line x1="18" y1="20" x2="18" y2="10" /><line x1="12" y1="20" x2="12" y2="4" /><line x1="6" y1="20" x2="6" y2="14" /></svg></Icon>;
const IconZap = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" /></svg></Icon>;
const IconClock = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></svg></Icon>;
const IconHardDrive = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><line x1="22" y1="12" x2="2" y2="12" /><path d="M5.45 5.11L2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z" /><line x1="6" y1="16" x2="6.01" y2="16" /><line x1="10" y1="16" x2="10.01" y2="16" /></svg></Icon>;
const IconCpu = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><rect x="4" y="4" width="16" height="16" rx="2" /><rect x="9" y="9" width="6" height="6" /><line x1="9" y1="1" x2="9" y2="4" /><line x1="15" y1="1" x2="15" y2="4" /><line x1="9" y1="20" x2="9" y2="23" /><line x1="15" y1="20" x2="15" y2="23" /><line x1="20" y1="9" x2="23" y2="9" /><line x1="20" y1="14" x2="23" y2="14" /><line x1="1" y1="9" x2="4" y2="9" /><line x1="1" y1="14" x2="4" y2="14" /></svg></Icon>;
const IconLayers = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><polygon points="12 2 2 7 12 12 22 7 12 2" /><polyline points="2 17 12 22 22 17" /><polyline points="2 12 12 17 22 12" /></svg></Icon>;
const IconActivity = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12" /></svg></Icon>;
const IconServer = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><rect x="2" y="2" width="20" height="8" rx="2" ry="2" /><rect x="2" y="14" width="20" height="8" rx="2" ry="2" /><line x1="6" y1="6" x2="6.01" y2="6" /><line x1="6" y1="18" x2="6.01" y2="18" /></svg></Icon>;
const IconSearch = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" /></svg></Icon>;
const IconSettings = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="3" /><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" /></svg></Icon>;
const IconList = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><line x1="8" y1="6" x2="21" y2="6" /><line x1="8" y1="12" x2="21" y2="12" /><line x1="8" y1="18" x2="21" y2="18" /><line x1="3" y1="6" x2="3.01" y2="6" /><line x1="3" y1="12" x2="3.01" y2="12" /><line x1="3" y1="18" x2="3.01" y2="18" /></svg></Icon>;
const IconKey = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4" /></svg></Icon>;
const IconSun = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="5" /><line x1="12" y1="1" x2="12" y2="3" /><line x1="12" y1="21" x2="12" y2="23" /><line x1="4.22" y1="4.22" x2="5.64" y2="5.64" /><line x1="18.36" y1="18.36" x2="19.78" y2="19.78" /><line x1="1" y1="12" x2="3" y2="12" /><line x1="21" y1="12" x2="23" y2="12" /><line x1="4.22" y1="19.78" x2="5.64" y2="18.36" /><line x1="18.36" y1="5.64" x2="19.78" y2="4.22" /></svg></Icon>;
const IconMoon = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" /></svg></Icon>;
const IconBookOpen = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z" /><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z" /></svg></Icon>;
const IconEdit = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" /><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" /></svg></Icon>;
const IconRefresh = ({ size }) => <Icon size={size}><svg viewBox="0 0 24 24"><polyline points="23 4 23 10 17 10" /><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" /></svg></Icon>;

// ═══════════════════════════════════════════════════════════════
// SAMPLE QUERIES — grouped for demo
// ═══════════════════════════════════════════════════════════════
const QUICK_QUERIES = [
  {
    group: 'DDL — Schema', items: [
      { label: 'Create Users Table', sql: `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(200), age INT)` },
      { label: 'Create Orders Table', sql: `CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product VARCHAR(100), amount DOUBLE)` },
      { label: 'Show All Tables', sql: `SHOW TABLES` },
    ]
  },
  {
    group: 'DML — Insert', items: [
      { label: 'Insert User — Alice', sql: `INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 28)` },
      { label: 'Insert User — Bob', sql: `INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 34)` },
      { label: 'Insert User — Charlie', sql: `INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 22)` },
      { label: 'Insert Order #1', sql: `INSERT INTO orders VALUES (1, 1, 'Widget Pro', 29.99)` },
      { label: 'Insert Order #2', sql: `INSERT INTO orders VALUES (2, 2, 'Gadget Max', 49.99)` },
    ]
  },
  {
    group: 'DQL — Query', items: [
      { label: 'Select All Users', sql: `SELECT * FROM users` },
      { label: 'Select with WHERE', sql: `SELECT name, email FROM users WHERE age > 25` },
      { label: 'Count Users', sql: `SELECT COUNT(*) FROM users` },
      { label: 'Select All Orders', sql: `SELECT * FROM orders` },
      { label: 'Sum Order Amounts', sql: `SELECT SUM(amount) FROM orders` },
    ]
  },
];

// ═══════════════════════════════════════════════════════════════
// APP — Root component
// ═══════════════════════════════════════════════════════════════
function App() {
  const [activeTab, setActiveTab] = useState('query');
  const [isConnected, setIsConnected] = useState(false);
  const [theme, setTheme] = useState(() => {
    return localStorage.getItem('samanvay-theme') || 'dark';
  });

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('samanvay-theme', theme);
  }, [theme]);

  const toggleTheme = () => {
    setTheme(prev => prev === 'dark' ? 'light' : 'dark');
  };

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

  // Shared callback to set SQL from quick queries
  const [pendingSQL, setPendingSQL] = useState(null);

  return (
    <>
      <nav className="navbar">
        <div className="navbar-left">
          <div className="brand">
            <div className="brand-icon">S</div>
            <div className="brand-text">
              <span className="brand-name">Samanvay</span>
              <span className="brand-sub">HTAP Database</span>
            </div>
          </div>
          <div className="nav-tabs">
            <button
              className={`nav-tab ${activeTab === 'query' ? 'active' : ''}`}
              onClick={() => setActiveTab('query')}
            >
              <span className="nav-tab-icon"><IconTerminal size={13} /></span> Query Editor
            </button>
            <button
              className={`nav-tab ${activeTab === 'dashboard' ? 'active' : ''}`}
              onClick={() => setActiveTab('dashboard')}
            >
              <span className="nav-tab-icon"><IconBarChart size={13} /></span> Dashboard
            </button>
          </div>
        </div>
        <div className="navbar-right">
          <span className="theme-label">{theme === 'dark' ? <IconMoon size={12} /> : <IconSun size={12} />}</span>
          <button className="theme-toggle" onClick={toggleTheme} title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}>
            <div className="theme-toggle-knob" />
          </button>
          <div className={`connection-badge ${isConnected ? 'connected' : 'disconnected'}`}>
            <span className="status-dot"></span>
            {isConnected ? 'Connected' : 'Disconnected'}
          </div>
        </div>
      </nav>

      <div className="app-layout">
        <Sidebar onSelectQuery={(sql) => { setPendingSQL(sql); setActiveTab('query'); }} />
        {activeTab === 'query' ? (
          <QueryEditorView pendingSQL={pendingSQL} onConsumeSQL={() => setPendingSQL(null)} />
        ) : (
          <DashboardView />
        )}
      </div>
    </>
  );
}

// ═══════════════════════════════════════════════════════════════
// SIDEBAR — Object Browser + Quick Queries
// ═══════════════════════════════════════════════════════════════
function Sidebar({ onSelectQuery }) {
  const [tables, setTables] = useState([]);
  const [schemas, setSchemas] = useState({});
  const [expanded, setExpanded] = useState({});
  const [loading, setLoading] = useState(true);

  const fetchTables = useCallback(async () => {
    try {
      const res = await listTables();
      if (res.success && res.data?.tables) {
        setTables(res.data.tables);
      }
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => {
    fetchTables();
    const interval = setInterval(fetchTables, 8000);
    return () => clearInterval(interval);
  }, [fetchTables]);

  const toggleTable = async (name) => {
    const isOpen = expanded[name];
    setExpanded(prev => ({ ...prev, [name]: !isOpen }));

    if (!isOpen && !schemas[name]) {
      try {
        const res = await getTableSchema(name);
        if (res.success && res.data) {
          setSchemas(prev => ({ ...prev, [name]: res.data }));
        }
      } catch { /* ignore */ }
    }
  };

  return (
    <aside className="sidebar">
      {/* Object browser */}
      <div className="sidebar-header">
        <span className="sidebar-header-icon"><IconDatabase size={13} /></span>
        <h3>Explorer</h3>
        <span className="table-count">{tables.length}</span>
      </div>
      <div className="sidebar-tree">
        <div className="tree-root">
          <span className="tree-root-icon"><IconServer size={13} /></span>
          samanvay_db
        </div>

        {loading ? (
          <div className="sidebar-loading">
            <span className="spinner"></span> Loading...
          </div>
        ) : tables.length === 0 ? (
          <div className="sidebar-empty">No tables yet. Run CREATE TABLE to start.</div>
        ) : (
          tables.map(name => (
            <div key={name}>
              <div
                className={`tree-table ${expanded[name] ? 'expanded' : ''}`}
                onClick={() => toggleTable(name)}
              >
                <span className="tree-table-icon"><IconTable size={12} /></span>
                <span className="tree-table-name">{name}</span>
                <span className={`tree-chevron ${expanded[name] ? 'open' : ''}`}>
                  <IconChevron size={10} />
                </span>
              </div>
              {expanded[name] && schemas[name] && (
                <div className="tree-columns">
                  {schemas[name].columns?.map(col => (
                    <div key={col.name} className="tree-column">
                      <span className="tree-column-icon"><IconColumns size={10} /></span>
                      <span className="tree-column-name">{col.name}</span>
                      {col.name === schemas[name].primaryKey && (
                        <span className="tree-pk-badge">PK</span>
                      )}
                      <span className="tree-column-type">{col.type}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))
        )}
      </div>

      {/* Quick Queries */}
      <div className="quick-queries-panel">
        <div className="quick-queries-header">
          <span className="sidebar-header-icon"><IconZap size={13} /></span>
          <h3>Quick Queries</h3>
        </div>
        <div className="quick-queries-list">
          {QUICK_QUERIES.map((group) => (
            <div key={group.group}>
              <div className="quick-query-group">{group.group}</div>
              {group.items.map((q, i) => (
                <div key={i} className="quick-query-item" onClick={() => onSelectQuery(q.sql)}>
                  <div className="quick-query-label">{q.label}</div>
                  <div className="quick-query-sql">{q.sql}</div>
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
    </aside>
  );
}

// ═══════════════════════════════════════════════════════════════
// QUERY EDITOR VIEW
// ═══════════════════════════════════════════════════════════════
function QueryEditorView({ pendingSQL, onConsumeSQL }) {
  const [sql, setSql] = useState('');
  const [results, setResults] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [resultTab, setResultTab] = useState('data');
  const textareaRef = useRef(null);
  const resultsEndRef = useRef(null);

  // Handle pending SQL from quick queries
  useEffect(() => {
    if (pendingSQL) {
      setSql(pendingSQL);
      onConsumeSQL();
      textareaRef.current?.focus();
    }
  }, [pendingSQL, onConsumeSQL]);

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
    } finally {
      setIsLoading(false);
      textareaRef.current?.focus();
    }
  }, [sql, isLoading]);

  const handleKeyDown = useCallback((e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      runQuery();
    }
    if (e.key === 'Tab') {
      e.preventDefault();
      const start = e.target.selectionStart;
      const end = e.target.selectionEnd;
      const newVal = sql.substring(0, start) + '  ' + sql.substring(end);
      setSql(newVal);
      setTimeout(() => {
        e.target.selectionStart = e.target.selectionEnd = start + 2;
      }, 0);
    }
  }, [runQuery, sql]);

  const clearResults = () => setResults([]);

  const lineCount = Math.max(sql.split('\n').length, 6);
  const successCount = results.filter(r => r.response.success).length;
  const errorCount = results.filter(r => !r.response.success).length;

  return (
    <div className="main-content">
      {/* Editor */}
      <div className="editor-panel">
        <div className="editor-toolbar">
          <div className="toolbar-actions">
            <button
              className="btn btn-execute"
              onClick={runQuery}
              disabled={!sql.trim() || isLoading}
            >
              {isLoading ? (
                <><span className="spinner"></span> Running...</>
              ) : (
                <><IconPlay size={11} /> Execute</>
              )}
            </button>
            <button className="btn btn-ghost" onClick={clearResults}>
              <IconTrash size={11} /> Clear
            </button>
          </div>
          <div className="toolbar-info">
            <span className="shortcut-hint">Cmd+Enter to run</span>
          </div>
        </div>
        <div className="editor-area">
          <div className="line-numbers">
            {Array.from({ length: lineCount }, (_, i) => (
              <span key={i} className="line-number">{i + 1}</span>
            ))}
          </div>
          <textarea
            ref={textareaRef}
            className="code-textarea"
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={"-- Enter SQL query here...\n-- Example: SELECT * FROM users WHERE age > 25"}
            spellCheck={false}
            autoFocus
          />
        </div>
      </div>

      {/* Results */}
      <div className="results-panel">
        <div className="results-tabs">
          <button
            className={`results-tab ${resultTab === 'data' ? 'active' : ''}`}
            onClick={() => setResultTab('data')}
          >
            <IconGrid size={12} /> Data Output
            {results.length > 0 && <span className="results-tab-badge">{results.length}</span>}
          </button>
          <button
            className={`results-tab ${resultTab === 'messages' ? 'active' : ''}`}
            onClick={() => setResultTab('messages')}
          >
            <IconMessageSquare size={12} /> Messages
            {(successCount + errorCount) > 0 && (
              <span className="results-tab-badge">
                {successCount > 0 && <span style={{ color: 'var(--text-success)' }}>{successCount}ok</span>}
                {errorCount > 0 && <span style={{ color: 'var(--text-error)', marginLeft: successCount > 0 ? 4 : 0 }}>{errorCount}err</span>}
              </span>
            )}
          </button>
        </div>
        <div className="results-body">
          {resultTab === 'data' ? (
            results.length === 0 ? (
              <div className="empty-state">
                <span className="empty-state-icon"><IconTerminal size={32} /></span>
                <div className="empty-state-text">Execute a query to see results here</div>
                <div className="empty-state-hint">SELECT * FROM your_table</div>
              </div>
            ) : (
              <div className="results-list">
                <ResultEntry entry={results[results.length - 1]} />
              </div>
            )
          ) : (
            <div className="messages-log">
              {results.length === 0 ? (
                <div className="empty-state">
                  <span className="empty-state-icon"><IconMessageSquare size={32} /></span>
                  <div className="empty-state-text">No messages yet</div>
                </div>
              ) : (
                results.map(entry => (
                  <div key={entry.id} className={`msg-line ${entry.response.success ? 'success' : 'error'}`}>
                    <span className="msg-time">[{entry.timestamp}]</span>
                    <span>
                      {entry.response.success ? 'OK' : 'ERR'}{' '}
                      {entry.sql}
                      {entry.elapsed > 0 && <span style={{ color: 'var(--text-muted)' }}> — {entry.elapsed}ms</span>}
                    </span>
                  </div>
                ))
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// RESULT ENTRY
// ═══════════════════════════════════════════════════════════════
function ResultEntry({ entry }) {
  const { sql, response, elapsed } = entry;

  return (
    <div className="result-entry">
      <div className="result-query-bar">
        <span className="result-query-prompt">&gt;</span>
        <code className="result-query-sql">{sql}</code>
        <span className="result-query-time">{elapsed}ms</span>
      </div>

      {!response.success ? (
        <div className="result-msg error">
          <span className="result-msg-icon">x</span>
          <div className="result-msg-body">
            <strong>{response.error?.type || 'ERROR'}:</strong>{' '}
            {response.error?.message || 'Unknown error'}
          </div>
        </div>
      ) : response.data?.headers?.length > 0 ? (
        <div className="result-table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th className="row-num">#</th>
                {response.data.headers.map((h, i) => (
                  <th key={i}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {response.data.rows.length === 0 ? (
                <tr>
                  <td colSpan={response.data.headers.length + 1} style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '16px' }}>
                    No rows returned
                  </td>
                </tr>
              ) : (
                response.data.rows.map((row, ri) => (
                  <tr key={ri}>
                    <td className="row-num">{ri + 1}</td>
                    {row.map((cell, ci) => (
                      <td key={ci}>{cell}</td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
          <div className="result-footer">
            <span className="result-footer-item">
              {response.data.rows.length} row{response.data.rows.length !== 1 ? 's' : ''} returned
            </span>
            <span className="result-footer-item">
              {elapsed}ms
            </span>
          </div>
        </div>
      ) : (
        <div className="result-msg success">
          <span className="result-msg-icon">ok</span>
          <div className="result-msg-body">
            {response.data?.message || `OK — ${response.data?.rowsAffected || 0} row(s) affected`}
          </div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// DASHBOARD VIEW
// ═══════════════════════════════════════════════════════════════
function DashboardView() {
  const [stats, setStats] = useState(null);
  const [info, setInfo] = useState(null);
  const [tables, setTables] = useState([]);
  const [schemas, setSchemas] = useState({});
  const [loading, setLoading] = useState(true);

  const fetchAll = useCallback(async () => {
    try {
      const [statusRes, infoRes, tablesRes] = await Promise.all([
        getStatus(),
        getInfo(),
        listTables(),
      ]);
      if (statusRes.success) setStats(statusRes.data);
      if (infoRes.success) setInfo(infoRes.data);
      if (tablesRes.success && tablesRes.data?.tables) {
        setTables(tablesRes.data.tables);
        const schemaPromises = tablesRes.data.tables.map(async (name) => {
          try {
            const res = await getTableSchema(name);
            if (res.success) return { name, schema: res.data };
          } catch { /* ignore */ }
          return null;
        });
        const results = await Promise.all(schemaPromises);
        const newSchemas = {};
        results.forEach(r => { if (r) newSchemas[r.name] = r.schema; });
        setSchemas(newSchemas);
      }
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => {
    fetchAll();
    const interval = setInterval(fetchAll, 6000);
    return () => clearInterval(interval);
  }, [fetchAll]);

  const formatBytes = (bytes) => {
    if (bytes === 0 || bytes == null) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  };

  const formatNum = (n) => {
    if (n == null) return '0';
    return n.toLocaleString();
  };

  if (loading) {
    return (
      <div className="main-content">
        <div className="empty-state" style={{ flex: 1 }}>
          <span className="spinner" style={{ width: 28, height: 28 }}></span>
          <div className="empty-state-text">Loading dashboard...</div>
        </div>
      </div>
    );
  }

  return (
    <div className="main-content">
      <div className="dashboard">
        {/* Operations */}
        <div className="dashboard-section">
          <div className="dashboard-section-title">Engine Statistics</div>
          <div className="stat-cards">
            <div className="stat-card blue">
              <div className="stat-card-header">
                <span className="stat-card-label">Total Reads</span>
                <span className="stat-card-icon"><IconBookOpen size={14} /></span>
              </div>
              <div className="stat-card-value">{formatNum(stats?.operations?.totalReads)}</div>
              <div className="stat-card-sub">Point lookups</div>
            </div>
            <div className="stat-card green">
              <div className="stat-card-header">
                <span className="stat-card-label">Total Writes</span>
                <span className="stat-card-icon"><IconEdit size={14} /></span>
              </div>
              <div className="stat-card-value">{formatNum(stats?.operations?.totalWrites)}</div>
              <div className="stat-card-sub">Insert / Update / Delete</div>
            </div>
            <div className="stat-card purple">
              <div className="stat-card-header">
                <span className="stat-card-label">Range Queries</span>
                <span className="stat-card-icon"><IconSearch size={14} /></span>
              </div>
              <div className="stat-card-value">{formatNum(stats?.operations?.totalRangeQueries)}</div>
              <div className="stat-card-sub">Analytical scans</div>
            </div>
            <div className="stat-card amber">
              <div className="stat-card-header">
                <span className="stat-card-label">Compactions</span>
                <span className="stat-card-icon"><IconRefresh size={14} /></span>
              </div>
              <div className="stat-card-value">{formatNum(stats?.operations?.totalCompactions)}</div>
              <div className="stat-card-sub">LSM-tree merges</div>
            </div>
          </div>
        </div>

        {/* Performance */}
        <div className="dashboard-section">
          <div className="dashboard-section-title">Performance & Storage</div>
          <div className="stat-cards">
            <div className="stat-card cyan">
              <div className="stat-card-header">
                <span className="stat-card-label">Avg Read Latency</span>
                <span className="stat-card-icon"><IconClock size={14} /></span>
              </div>
              <div className="stat-card-value">{stats?.performance?.avgReadLatencyUs != null ? stats.performance.avgReadLatencyUs.toFixed(1) : '—'}<span style={{ fontSize: 14, color: 'var(--text-muted)' }}> us</span></div>
            </div>
            <div className="stat-card cyan">
              <div className="stat-card-header">
                <span className="stat-card-label">Avg Write Latency</span>
                <span className="stat-card-icon"><IconClock size={14} /></span>
              </div>
              <div className="stat-card-value">{stats?.performance?.avgWriteLatencyUs != null ? stats.performance.avgWriteLatencyUs.toFixed(1) : '—'}<span style={{ fontSize: 14, color: 'var(--text-muted)' }}> us</span></div>
            </div>
            <div className="stat-card blue">
              <div className="stat-card-header">
                <span className="stat-card-label">Memory Usage</span>
                <span className="stat-card-icon"><IconCpu size={14} /></span>
              </div>
              <div className="stat-card-value">{formatBytes(stats?.memory?.memtableMemoryUsage)}</div>
              <div className="stat-card-sub">{stats?.memory?.activeMemtables ?? 0} active, {stats?.memory?.frozenMemtables ?? 0} frozen memtables</div>
            </div>
            <div className="stat-card green">
              <div className="stat-card-header">
                <span className="stat-card-label">SSTable Size</span>
                <span className="stat-card-icon"><IconHardDrive size={14} /></span>
              </div>
              <div className="stat-card-value">{formatBytes(stats?.sstables?.totalBytes)}</div>
              <div className="stat-card-sub">{stats?.sstables?.totalCount ?? 0} files on disk</div>
            </div>
            <div className="stat-card red">
              <div className="stat-card-header">
                <span className="stat-card-label">Columnar Store</span>
                <span className="stat-card-icon"><IconBarChart size={14} /></span>
              </div>
              <div className="stat-card-value">{formatNum(stats?.columnar?.rows)}</div>
              <div className="stat-card-sub">{stats?.columnar?.files ?? 0} columnar files</div>
            </div>
          </div>
        </div>

        {/* Info panels */}
        <div className="dashboard-section">
          <div className="dashboard-section-title">Engine Info</div>
          <div className="info-grid">
            {stats?.sstables?.levels?.length > 0 && (
              <div className="info-panel">
                <div className="info-panel-header">
                  <span className="info-panel-header-icon"><IconLayers size={13} /></span>
                  LSM-Tree Levels
                </div>
                <div className="info-panel-body" style={{ padding: 0 }}>
                  <table className="level-table">
                    <thead>
                      <tr>
                        <th>Level</th>
                        <th>SSTables</th>
                        <th>Size</th>
                        <th>Entries</th>
                      </tr>
                    </thead>
                    <tbody>
                      {stats.sstables.levels.map(l => (
                        <tr key={l.level}>
                          <td>L{l.level}</td>
                          <td>{l.numSSTables}</td>
                          <td>{formatBytes(l.totalSize)}</td>
                          <td>{formatNum(l.totalEntries)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {info && (
              <div className="info-panel">
                <div className="info-panel-header">
                  <span className="info-panel-header-icon"><IconSettings size={13} /></span>
                  Engine Details
                </div>
                <div className="info-panel-body">
                  <div className="info-row">
                    <span className="info-row-label">Engine</span>
                    <span className="info-row-value">{info.engine}</span>
                  </div>
                  <div className="info-row">
                    <span className="info-row-label">Version</span>
                    <span className="info-row-value">v{info.version}</span>
                  </div>

                  <div style={{ marginTop: 10, marginBottom: 4, fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>
                    Supported SQL
                  </div>
                  <div className="tag-list">
                    {info.supportedStatements?.map((s, i) => (
                      <span key={i} className="tag">{s}</span>
                    ))}
                  </div>

                  <div style={{ marginTop: 10, marginBottom: 4, fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>
                    Data Types
                  </div>
                  <div className="tag-list">
                    {info.supportedTypes?.map((t, i) => (
                      <span key={i} className="tag purple">{t}</span>
                    ))}
                  </div>

                  <div style={{ marginTop: 10, marginBottom: 4, fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>
                    Features
                  </div>
                  <div className="tag-list">
                    {info.features?.map((f, i) => (
                      <span key={i} className="tag green">{f}</span>
                    ))}
                  </div>
                </div>
              </div>
            )}

            <div className="info-panel">
              <div className="info-panel-header">
                <span className="info-panel-header-icon"><IconTable size={13} /></span>
                Registered Tables ({tables.length})
              </div>
              <div className="info-panel-body" style={{ padding: 0 }}>
                {tables.length === 0 ? (
                  <div className="sidebar-empty" style={{ padding: 20 }}>No tables created yet</div>
                ) : (
                  <div className="table-browser">
                    {tables.map(name => (
                      <div key={name} className="table-browser-item">
                        <div className="table-browser-name">
                          <span className="table-browser-name-icon"><IconTable size={12} /></span>
                          {name}
                          {schemas[name] && (
                            <span style={{ fontSize: 10, color: 'var(--text-muted)', fontWeight: 400 }}>
                              — {schemas[name].columns?.length ?? 0} columns
                            </span>
                          )}
                        </div>
                        {schemas[name]?.columns && (
                          <div className="table-browser-cols">
                            {schemas[name].columns.map(c => (
                              <span key={c.name} className="table-browser-col">
                                {c.name === schemas[name].primaryKey ? '[PK] ' : ''}{c.name}: {c.type}
                              </span>
                            ))}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
