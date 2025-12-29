/**
 * PROJECT SAMANVAY - HTAP Database Dashboard
 * Frontend JavaScript with Charts
 */

// API Configuration
const API_BASE = '';

// State
let state = {
    lsmLevels: [],
    memtableEntries: [],
    sstables: [],
    stats: {},
    tests: [],
    isTestRunning: false,
    opsHistory: []
};

// Charts
let lsmChart = null;
let opsChart = null;

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    console.log('🚀 Project Samanvay Dashboard Initializing...');

    // Initialize charts
    initCharts();

    // Load initial data
    loadAllData();

    // Load available tests
    loadTests();

    // Start real-time updates
    startRealtimeUpdates();

    // Start clock
    updateClock();
    setInterval(updateClock, 1000);

    // Start uptime counter
    const startTime = Date.now();
    setInterval(() => updateUptime(startTime), 1000);
});

// ═══════════════════════════════════════════════════════════════
// CHARTS INITIALIZATION
// ═══════════════════════════════════════════════════════════════

function initCharts() {
    // LSM Level Size Chart
    const lsmCtx = document.getElementById('lsm-chart');
    if (lsmCtx) {
        lsmChart = new Chart(lsmCtx, {
            type: 'bar',
            data: {
                labels: ['L0', 'L1', 'L2', 'L3', 'L4', 'L5', 'L6'],
                datasets: [{
                    label: 'SSTables',
                    data: [0, 0, 0, 0, 0, 0, 0],
                    backgroundColor: [
                        '#2563eb', '#2563eb', '#2563eb', '#2563eb',
                        '#22c55e', '#22c55e', '#22c55e'
                    ],
                    borderRadius: 4,
                    barThickness: 20
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { color: '#e2e8f0' },
                        ticks: { color: '#64748b', stepSize: 1 }
                    },
                    x: {
                        grid: { display: false },
                        ticks: { color: '#64748b' }
                    }
                }
            }
        });
    }

    // Operations Line Chart
    const opsCtx = document.getElementById('ops-chart');
    if (opsCtx) {
        opsChart = new Chart(opsCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Ops/sec',
                    data: [],
                    borderColor: '#2563eb',
                    backgroundColor: 'rgba(37, 99, 235, 0.1)',
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { color: '#e2e8f0' },
                        ticks: {
                            color: '#64748b',
                            callback: function (value) {
                                return formatNumber(value);
                            }
                        }
                    },
                    x: {
                        display: false
                    }
                }
            }
        });
    }
}

function updateCharts() {
    // Update LSM Chart
    if (lsmChart && state.lsmLevels) {
        const sstableCounts = state.lsmLevels.map(level => level.num_sstables || 0);
        lsmChart.data.datasets[0].data = sstableCounts;
        lsmChart.update('none');
    }

    // Update Ops Chart
    if (opsChart && state.stats) {
        const now = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        state.opsHistory.push({
            time: now,
            value: state.stats.ops_per_second || 0
        });

        // Keep only last 20 points
        if (state.opsHistory.length > 20) {
            state.opsHistory.shift();
        }

        opsChart.data.labels = state.opsHistory.map(h => h.time);
        opsChart.data.datasets[0].data = state.opsHistory.map(h => h.value);
        opsChart.update('none');
    }
}

// ═══════════════════════════════════════════════════════════════
// DATA FETCHING
// ═══════════════════════════════════════════════════════════════

async function loadAllData() {
    try {
        await Promise.all([
            loadState(),
            loadMemtable(),
            loadSSTables()
        ]);
        updateCharts();
    } catch (error) {
        console.error('Error loading data:', error);
    }
}

async function loadState() {
    try {
        const response = await fetch(`${API_BASE}/api/state`);
        const data = await response.json();
        state.lsmLevels = data.lsm_levels;
        state.stats = data.stats;
        renderLSMTree();
        updateStats();
    } catch (error) {
        console.error('Error loading state:', error);
    }
}

async function loadMemtable() {
    try {
        const response = await fetch(`${API_BASE}/api/memtable`);
        const data = await response.json();
        state.memtableEntries = data.entries;
        renderMemtable();
        document.getElementById('memtable-count').textContent = `${data.count} entries`;
    } catch (error) {
        console.error('Error loading memtable:', error);
    }
}

async function loadSSTables() {
    try {
        const response = await fetch(`${API_BASE}/api/sstables`);
        const data = await response.json();
        state.sstables = data.sstables;
        renderSSTables();
        document.getElementById('stat-sstables').textContent = data.count;
        document.getElementById('stat-storage').textContent = `${data.total_size_mb.toFixed(1)} MB`;
        document.getElementById('sstable-badge').textContent = `${data.count} tables`;
    } catch (error) {
        console.error('Error loading SSTables:', error);
    }
}

function renderSSTables() {
    const container = document.getElementById('sstable-list');

    if (!state.sstables || state.sstables.length === 0) {
        container.innerHTML = '<div class="level-empty">No SSTables found</div>';
        return;
    }

    // Sort by level
    const sorted = [...state.sstables].sort((a, b) => {
        if (a.level !== b.level) return a.level - b.level;
        return b.creation_time - a.creation_time;
    });

    container.innerHTML = sorted.map(sst => `
        <div class="sstable-item ${sst.is_columnar ? 'columnar' : 'row'}"
             onclick="showSSTableDetails('${sst.id}')">
            <span class="sstable-level-badge">L${sst.level}</span>
            <span class="sstable-path">${sst.file_path}</span>
            <span class="sstable-size">${formatSize(sst.file_size_kb)}</span>
        </div>
    `).join('');
}

async function loadTests() {
    try {
        const response = await fetch(`${API_BASE}/api/tests`);
        const data = await response.json();
        state.tests = data.tests;
        renderTestSelector();
    } catch (error) {
        console.error('Error loading tests:', error);
    }
}

function startRealtimeUpdates() {
    setInterval(() => {
        if (!state.isTestRunning) {
            loadAllData();
        }
    }, 3000);
}

// ═══════════════════════════════════════════════════════════════
// RENDERING FUNCTIONS
// ═══════════════════════════════════════════════════════════════

function renderLSMTree() {
    const container = document.getElementById('lsm-tree');
    if (!state.lsmLevels || state.lsmLevels.length === 0) {
        container.innerHTML = '<div class="level-empty">Loading LSM tree data...</div>';
        return;
    }

    container.innerHTML = state.lsmLevels.map((level, idx) => {
        const isColumnar = level.is_columnar;
        const sstablesHtml = level.sstables && level.sstables.length > 0
            ? level.sstables.map(sst => `
                <div class="sstable-block ${isColumnar ? 'columnar' : ''}" 
                     onclick="showSSTableDetails('${sst.id}')"
                     title="${sst.file_path}">
                    <span class="sstable-id">${sst.id}</span>
                    <span class="sstable-info">${formatSize(sst.file_size_kb)} • ${formatNumber(sst.entry_count)}</span>
                </div>
            `).join('')
            : '<span class="level-empty">Empty</span>';

        return `
            <div class="lsm-level">
                <div class="level-label ${isColumnar ? 'columnar' : ''}">
                    <span class="level-num">L${idx}</span>
                    <span class="level-type">${level.format}</span>
                </div>
                <div class="level-sstables">
                    ${sstablesHtml}
                </div>
            </div>
        `;
    }).join('');
}

function renderMemtable() {
    const container = document.getElementById('memtable-entries');

    if (!state.memtableEntries || state.memtableEntries.length === 0) {
        container.innerHTML = '<div class="level-empty">Memtable is empty</div>';
        return;
    }

    container.innerHTML = state.memtableEntries.map(entry => `
        <div class="memtable-entry ${entry.deleted ? 'deleted' : ''}">
            <span class="entry-key">${escapeHtml(entry.key)}</span>
            ${entry.deleted
            ? '<span class="entry-tombstone">TOMBSTONE</span>'
            : `<span class="entry-value" title="${escapeHtml(entry.value)}">${escapeHtml(truncate(entry.value, 35))}</span>`
        }
            <span class="entry-seq">seq:${entry.seq}</span>
        </div>
    `).join('');
}

function renderTestSelector() {
    const selector = document.getElementById('test-selector');
    selector.innerHTML = '<option value="">-- Select Test --</option>' +
        state.tests.map(test => `
            <option value="${test.id}" title="${test.description}">${test.name}</option>
        `).join('');
}

function updateStats() {
    if (state.stats) {
        document.getElementById('stat-writes').textContent = formatNumber(state.stats.total_writes);
        document.getElementById('stat-reads').textContent = formatNumber(state.stats.total_reads);
        document.getElementById('stat-range').textContent = formatNumber(state.stats.total_range_queries);
        document.getElementById('ops-sec').textContent = formatNumber(state.stats.ops_per_second);
    }
}

// ═══════════════════════════════════════════════════════════════
// ACTIONS
// ═══════════════════════════════════════════════════════════════

async function insertDemoEntry() {
    const key = `demo:${Date.now()}`;
    const value = JSON.stringify({
        timestamp: Date.now(),
        random: Math.random().toFixed(4),
        message: 'Demo entry'
    });

    try {
        const response = await fetch(`${API_BASE}/api/demo/insert`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ key, value })
        });
        const data = await response.json();
        if (data.success) {
            await loadMemtable();
            await loadState();
            updateCharts();
        }
    } catch (error) {
        console.error('Error inserting entry:', error);
    }
}

async function flushMemtable() {
    try {
        const response = await fetch(`${API_BASE}/api/demo/flush`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        const data = await response.json();
        if (data.success) {
            await loadAllData();
        } else if (data.error) {
            alert(data.error);
        }
    } catch (error) {
        console.error('Error flushing memtable:', error);
    }
}

async function showSSTableDetails(sstableId) {
    try {
        const response = await fetch(`${API_BASE}/api/sstable/${sstableId}`);
        const data = await response.json();

        const modal = document.getElementById('sstable-modal');
        document.getElementById('modal-title').textContent = `SSTable: ${sstableId}`;

        const body = document.getElementById('modal-body');
        body.innerHTML = `
            <div class="modal-section">
                <h4 style="color: #2563eb; margin-bottom: 12px; font-size: 13px; font-weight: 600;">METADATA</h4>
                <div class="modal-entry">
                    <span class="entry-key">File Path</span>
                    <span class="entry-value">${data.sstable.file_path}</span>
                </div>
                <div class="modal-entry">
                    <span class="entry-key">Level</span>
                    <span class="entry-value">L${data.sstable.level} (${data.sstable.is_columnar ? 'Columnar' : 'Row'})</span>
                </div>
                <div class="modal-entry">
                    <span class="entry-key">Key Range</span>
                    <span class="entry-value">${data.sstable.min_key} → ${data.sstable.max_key}</span>
                </div>
                <div class="modal-entry">
                    <span class="entry-key">Size</span>
                    <span class="entry-value">${formatSize(data.sstable.file_size_kb)}</span>
                </div>
                <div class="modal-entry">
                    <span class="entry-key">Entries</span>
                    <span class="entry-value">${formatNumber(data.sstable.entry_count)}</span>
                </div>
            </div>
            <div class="modal-section" style="margin-top: 20px;">
                <h4 style="color: #22c55e; margin-bottom: 12px; font-size: 13px; font-weight: 600;">SAMPLE ENTRIES (${data.showing} of ${formatNumber(data.total)})</h4>
                ${data.sample_entries.slice(0, 10).map(entry => `
                    <div class="modal-entry">
                        <span class="entry-key">${escapeHtml(entry.key)}</span>
                        <span class="entry-value">${escapeHtml(truncate(entry.value, 50))}</span>
                    </div>
                `).join('')}
            </div>
        `;

        modal.classList.add('active');
    } catch (error) {
        console.error('Error loading SSTable details:', error);
    }
}

function closeModal() {
    document.getElementById('sstable-modal').classList.remove('active');
}

async function runTest() {
    const selector = document.getElementById('test-selector');
    const testId = selector.value;

    if (!testId) {
        alert('Please select a test to run');
        return;
    }

    const statusBadge = document.getElementById('test-status');
    const runBtn = document.getElementById('run-test-btn');
    const outputContent = document.getElementById('test-output-content');

    state.isTestRunning = true;
    statusBadge.textContent = 'Running';
    statusBadge.classList.add('running');
    runBtn.disabled = true;
    outputContent.textContent = `Starting test: ${testId}...\n\n`;

    try {
        const response = await fetch(`${API_BASE}/api/run-test`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ test_id: testId })
        });
        const data = await response.json();

        if (data.success) {
            statusBadge.textContent = 'Passed';
            statusBadge.style.color = '#22c55e';
            outputContent.textContent = data.stdout || 'Test completed successfully';
        } else {
            statusBadge.textContent = 'Failed';
            statusBadge.style.color = '#ef4444';
            outputContent.textContent = data.error || data.stderr || 'Test failed';
        }

        if (data.stderr && data.stderr.length > 0) {
            outputContent.textContent += '\n\n--- STDERR ---\n' + data.stderr;
        }

        // Auto-scroll to bottom
        outputContent.scrollTop = outputContent.scrollHeight;

    } catch (error) {
        statusBadge.textContent = 'Error';
        statusBadge.style.color = '#ef4444';
        outputContent.textContent = `Error: ${error.message}`;
    } finally {
        state.isTestRunning = false;
        statusBadge.classList.remove('running');
        runBtn.disabled = false;

        setTimeout(() => {
            statusBadge.style.color = '';
            statusBadge.textContent = 'Idle';
        }, 5000);
    }
}

function clearTestOutput() {
    document.getElementById('test-output-content').textContent = 'Waiting for test execution...';
}

// ═══════════════════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ═══════════════════════════════════════════════════════════════

function updateClock() {
    const now = new Date();
    const time = now.toLocaleTimeString('en-US', { hour12: false });
    document.getElementById('timestamp').textContent = time;
}

function updateUptime(startTime) {
    const elapsed = Math.floor((Date.now() - startTime) / 1000);
    const hours = Math.floor(elapsed / 3600).toString().padStart(2, '0');
    const minutes = Math.floor((elapsed % 3600) / 60).toString().padStart(2, '0');
    const seconds = (elapsed % 60).toString().padStart(2, '0');
    document.getElementById('uptime').textContent = `${hours}:${minutes}:${seconds}`;
}

function formatNumber(num) {
    if (num === undefined || num === null) return '0';
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toString();
}

function formatSize(sizeKb) {
    if (sizeKb >= 1024 * 1024) return (sizeKb / (1024 * 1024)).toFixed(1) + ' GB';
    if (sizeKb >= 1024) return (sizeKb / 1024).toFixed(1) + ' MB';
    return sizeKb.toFixed(0) + ' KB';
}

function truncate(str, maxLen) {
    if (!str) return '';
    if (str.length <= maxLen) return str;
    return str.substring(0, maxLen) + '...';
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Close modal on outside click
document.addEventListener('click', (e) => {
    const modal = document.getElementById('sstable-modal');
    if (e.target === modal) {
        closeModal();
    }
});

// Keyboard shortcuts
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeModal();
    }
});

console.log('✅ Dashboard loaded successfully');
