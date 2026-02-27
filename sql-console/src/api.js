// API helper for Samanvay REST API
const API_BASE = 'http://localhost:8080';

export async function executeQuery(sql) {
    const res = await fetch(`${API_BASE}/api/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql }),
    });
    return res.json();
}

export async function explainQuery(sql) {
    const res = await fetch(`${API_BASE}/api/explain`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql }),
    });
    return res.json();
}

export async function listTables() {
    const res = await fetch(`${API_BASE}/api/tables`);
    return res.json();
}

export async function getTableSchema(name) {
    const res = await fetch(`${API_BASE}/api/tables/${encodeURIComponent(name)}`);
    return res.json();
}

export async function getStatus() {
    const res = await fetch(`${API_BASE}/api/status`);
    return res.json();
}

export async function checkHealth() {
    const res = await fetch(`${API_BASE}/api/health`);
    return res.json();
}

export async function getInfo() {
    const res = await fetch(`${API_BASE}/api/info`);
    return res.json();
}
