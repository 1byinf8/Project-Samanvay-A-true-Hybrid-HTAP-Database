# HTAP Dashboard Backend

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import subprocess
import os
import json
import time
import threading
from pathlib import Path

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)

# Configuration
STORAGE_ENGINE_DIR = Path(__file__).parent.parent / 'StorageEngine'
DATA_DIR = STORAGE_ENGINE_DIR / 'data'
TEST_DIR = STORAGE_ENGINE_DIR / 'test'

# Simulated state (for demo purposes - will be enhanced with real data)
class DatabaseState:
    def __init__(self):
        self.memtable_entries = []
        self.lsm_levels = []
        self.sstables = []
        self.operations_log = []
        self.stats = {
            'total_writes': 0,
            'total_reads': 0,
            'total_range_queries': 0,
            'memtable_size': 0,
            'ops_per_second': 0
        }
        self._init_demo_state()
    
    def _init_demo_state(self):
        """Initialize with realistic demo data"""
        # LSM Tree Configuration (7 levels)
        self.lsm_levels = []
        for level in range(7):
            is_columnar = level >= 4
            size_multiplier = 10 ** level
            base_size = 64  # 64MB base
            
            level_data = {
                'level': level,
                'is_columnar': is_columnar,
                'format': 'COLUMNAR' if is_columnar else 'ROW',
                'size_limit_mb': base_size * size_multiplier if level > 0 else 64,
                'current_size_mb': 0,
                'num_sstables': 0,
                'total_entries': 0,
                'sstables': []
            }
            self.lsm_levels.append(level_data)
        
        # Add some demo SSTables
        self._add_demo_sstables()
        
        # Add demo memtable entries
        self._add_demo_memtable_entries()
    
    def _add_demo_sstables(self):
        """Add realistic demo SSTables"""
        import random
        
        # Level 0: 3 SSTables (freshest)
        for i in range(3):
            sstable = {
                'id': f'L0_{i}',
                'file_path': f'./data/L0_{i}.sst',
                'level': 0,
                'min_key': f'key_{i*1000:06d}',
                'max_key': f'key_{(i+1)*1000-1:06d}',
                'file_size_kb': random.randint(800, 1200),
                'entry_count': random.randint(900, 1100),
                'creation_time': int(time.time()) - random.randint(60, 3600),
                'is_columnar': False
            }
            self.lsm_levels[0]['sstables'].append(sstable)
            self.sstables.append(sstable)
        
        # Level 1: 2 SSTables  
        for i in range(2):
            sstable = {
                'id': f'L1_{i}',
                'file_path': f'./data/L1_{i}.sst',
                'level': 1,
                'min_key': f'key_{i*5000:06d}',
                'max_key': f'key_{(i+1)*5000-1:06d}',
                'file_size_kb': random.randint(4000, 6000),
                'entry_count': random.randint(4500, 5500),
                'creation_time': int(time.time()) - random.randint(3600, 7200),
                'is_columnar': False
            }
            self.lsm_levels[1]['sstables'].append(sstable)
            self.sstables.append(sstable)
        
        # Level 2: 1 SSTable
        sstable = {
            'id': 'L2_0',
            'file_path': './data/L2_0.sst',
            'level': 2,
            'min_key': 'key_000000',
            'max_key': 'key_049999',
            'file_size_kb': random.randint(40000, 60000),
            'entry_count': random.randint(45000, 55000),
            'creation_time': int(time.time()) - random.randint(7200, 14400),
            'is_columnar': False
        }
        self.lsm_levels[2]['sstables'].append(sstable)
        self.sstables.append(sstable)
        
        # Level 4 (Columnar): 1 SSTable
        sstable = {
            'id': 'L4_0',
            'file_path': './data/L4_0.col',
            'level': 4,
            'min_key': 'key_000000',
            'max_key': 'key_499999',
            'file_size_kb': random.randint(200000, 300000),
            'entry_count': random.randint(450000, 550000),
            'creation_time': int(time.time()) - random.randint(86400, 172800),
            'is_columnar': True
        }
        self.lsm_levels[4]['sstables'].append(sstable)
        self.sstables.append(sstable)
        
        # Update level stats
        for level in self.lsm_levels:
            level['num_sstables'] = len(level['sstables'])
            level['total_entries'] = sum(s['entry_count'] for s in level['sstables'])
            level['current_size_mb'] = sum(s['file_size_kb'] for s in level['sstables']) / 1024
    
    def _add_demo_memtable_entries(self):
        """Add demo memtable entries"""
        entries = [
            {'key': 'user:1001', 'value': '{"name":"Alice","balance":5000}', 'seq': 1001, 'deleted': False},
            {'key': 'user:1002', 'value': '{"name":"Bob","balance":3200}', 'seq': 1002, 'deleted': False},
            {'key': 'user:1003', 'value': '{"name":"Charlie","balance":7800}', 'seq': 1003, 'deleted': False},
            {'key': 'order:5001', 'value': '{"product":"Widget A","qty":10}', 'seq': 1004, 'deleted': False},
            {'key': 'order:5002', 'value': '{"product":"Gadget B","qty":5}', 'seq': 1005, 'deleted': False},
            {'key': 'session:abc123', 'value': '{"user_id":1001,"expires":1735500000}', 'seq': 1006, 'deleted': False},
            {'key': 'cache:product:101', 'value': '{"name":"Widget","price":29.99}', 'seq': 1007, 'deleted': False},
            {'key': 'user:1000', 'value': '', 'seq': 1008, 'deleted': True},  # Tombstone
        ]
        self.memtable_entries = entries
        self.stats['memtable_size'] = len(entries)
        
        # Simulated operation counts
        self.stats['total_writes'] = 158432
        self.stats['total_reads'] = 892341
        self.stats['total_range_queries'] = 12456
        self.stats['ops_per_second'] = 45230

# Global state instance
db_state = DatabaseState()

# Test process tracking
running_tests = {}

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/api/state')
def get_state():
    """Get full database state"""
    return jsonify({
        'lsm_levels': db_state.lsm_levels,
        'stats': db_state.stats,
        'config': {
            'max_levels': 7,
            'columnar_threshold': 4,
            'level0_compaction_threshold': 4,
            'memtable_size_limit_mb': 64,
            'data_directory': './data'
        },
        'timestamp': int(time.time())
    })

@app.route('/api/memtable')
def get_memtable():
    """Get memtable entries"""
    return jsonify({
        'entries': db_state.memtable_entries,
        'count': len(db_state.memtable_entries),
        'size_bytes': sum(len(e['key']) + len(e['value']) for e in db_state.memtable_entries),
        'timestamp': int(time.time())
    })

@app.route('/api/sstables')
def get_sstables():
    """Get all SSTable metadata"""
    return jsonify({
        'sstables': db_state.sstables,
        'count': len(db_state.sstables),
        'total_size_mb': sum(s['file_size_kb'] for s in db_state.sstables) / 1024,
        'timestamp': int(time.time())
    })

@app.route('/api/sstable/<path:sstable_id>')
def get_sstable_content(sstable_id):
    """Get SSTable content (simulated sample)"""
    sstable = next((s for s in db_state.sstables if s['id'] == sstable_id), None)
    if not sstable:
        return jsonify({'error': 'SSTable not found'}), 404
    
    # Generate sample entries for this SSTable
    sample_entries = []
    for i in range(min(50, sstable['entry_count'])):
        sample_entries.append({
            'key': f"{sstable['min_key'][:-6]}{i:06d}",
            'value': f'{{"data": "sample_value_{i}", "ts": {int(time.time())}}}',
            'seq': sstable['creation_time'] + i
        })
    
    return jsonify({
        'sstable': sstable,
        'sample_entries': sample_entries,
        'showing': len(sample_entries),
        'total': sstable['entry_count']
    })

@app.route('/api/tests')
def get_tests():
    """Get available tests"""
    tests = [
        {'id': 'test_htap', 'name': 'HTAP Features Test', 'command': './test_htap', 'description': 'Validates LSM, SSTable, WAL, columnar format'},
        {'id': 'test_skiplist', 'name': 'Skiplist Unit Tests', 'command': './test_skiplist', 'description': 'Concurrent skiplist operations'},
        {'id': 'stress_test_quick', 'name': 'Quick Benchmark (100K)', 'command': './stress_test 100000 4', 'description': 'Fast benchmark with 100K records'},
        {'id': 'stress_test_full', 'name': 'Full Benchmark (1M)', 'command': './stress_test 1000000 8', 'description': 'Full benchmark with 1M records'},
    ]
    return jsonify({'tests': tests})

@app.route('/api/run-test', methods=['POST'])
def run_test():
    """Run a test and return output"""
    data = request.json
    test_id = data.get('test_id')
    
    test_commands = {
        'test_htap': './test_htap',
        'test_skiplist': './test_skiplist',
        'stress_test_quick': './stress_test 100000 4',
        'stress_test_full': './stress_test 1000000 8',
    }
    
    if test_id not in test_commands:
        return jsonify({'error': 'Unknown test'}), 400
    
    command = test_commands[test_id]
    
    try:
        # Run test in StorageEngine directory
        result = subprocess.run(
            command.split(),
            cwd=str(STORAGE_ENGINE_DIR),
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        return jsonify({
            'test_id': test_id,
            'command': command,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
            'success': result.returncode == 0,
            'timestamp': int(time.time())
        })
    except subprocess.TimeoutExpired:
        return jsonify({
            'test_id': test_id,
            'error': 'Test timed out after 5 minutes',
            'success': False
        })
    except FileNotFoundError:
        return jsonify({
            'test_id': test_id,
            'error': f'Test executable not found. Please compile first with: g++ -std=c++17 -O2 -o {command.split()[0]} test/{command.split()[0]}.cpp',
            'success': False
        })
    except Exception as e:
        return jsonify({
            'test_id': test_id,
            'error': str(e),
            'success': False
        })

@app.route('/api/demo/insert', methods=['POST'])
def demo_insert():
    """Demo: Insert a key-value pair"""
    data = request.json
    key = data.get('key', f'demo_key_{int(time.time())}')
    value = data.get('value', '{"demo": true}')
    
    entry = {
        'key': key,
        'value': value,
        'seq': len(db_state.memtable_entries) + 1000,
        'deleted': False
    }
    db_state.memtable_entries.insert(0, entry)
    db_state.stats['total_writes'] += 1
    db_state.stats['memtable_size'] = len(db_state.memtable_entries)
    
    return jsonify({'success': True, 'entry': entry})

@app.route('/api/demo/flush', methods=['POST'])
def demo_flush():
    """Demo: Flush memtable to Level 0 SSTable"""
    if not db_state.memtable_entries:
        return jsonify({'error': 'Memtable is empty'}), 400
    
    import random
    
    # Create new SSTable from memtable
    new_id = len(db_state.sstables)
    sstable = {
        'id': f'L0_{new_id}',
        'file_path': f'./data/L0_{new_id}.sst',
        'level': 0,
        'min_key': min(e['key'] for e in db_state.memtable_entries),
        'max_key': max(e['key'] for e in db_state.memtable_entries),
        'file_size_kb': len(db_state.memtable_entries) * random.randint(80, 120),
        'entry_count': len(db_state.memtable_entries),
        'creation_time': int(time.time()),
        'is_columnar': False
    }
    
    db_state.lsm_levels[0]['sstables'].insert(0, sstable)
    db_state.lsm_levels[0]['num_sstables'] += 1
    db_state.lsm_levels[0]['total_entries'] += sstable['entry_count']
    db_state.lsm_levels[0]['current_size_mb'] += sstable['file_size_kb'] / 1024
    db_state.sstables.append(sstable)
    
    # Clear memtable
    db_state.memtable_entries = []
    db_state.stats['memtable_size'] = 0
    
    return jsonify({'success': True, 'sstable': sstable})

if __name__ == '__main__':
    print("""
╔═══════════════════════════════════════════════════════════════════╗
║        PROJECT SAMANVAY - HTAP DATABASE DASHBOARD                 ║
╠═══════════════════════════════════════════════════════════════════╣
║  Server starting on http://localhost:5050                         ║
║  Open your browser to view the dashboard                          ║
╚═══════════════════════════════════════════════════════════════════╝
    """)
    app.run(host='0.0.0.0', port=5050, debug=True)
