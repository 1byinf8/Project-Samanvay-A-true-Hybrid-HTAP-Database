#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# demo_seed.sh — Populate Samanvay DB for demo presentation
# Creates OLTP + OLAP tables and pumps dashboard stats
# Usage: bash demo_seed.sh
# ═══════════════════════════════════════════════════════════════

API="http://localhost:8080/api/query"
GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
DIM='\033[0;90m'
NC='\033[0m'
BOLD='\033[1m'

query() {
  local sql="$1"
  local result=$(curl -s -X POST "$API" -H "Content-Type: application/json" -d "{\"sql\": \"$sql\"}")
  local success=$(echo "$result" | grep -o '"success":true' | head -1)
  if [ -n "$success" ]; then
    echo -e "  ${GREEN}OK${NC}  ${DIM}${sql:0:80}${NC}"
  else
    echo -e "  ${GREEN}--${NC}  ${DIM}${sql:0:80}${NC}"
  fi
}

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║     Samanvay Demo Seed — Populating Database     ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════╝${NC}"
echo ""

# ═══════════════════════════════════════════════════════════════
# PHASE 1: OLTP TABLES — Small, transactional
# ═══════════════════════════════════════════════════════════════
echo -e "${BLUE}[1/4] Creating OLTP tables...${NC}"

query "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(200), age INT)"
query "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product VARCHAR(100), amount DOUBLE)"
query "CREATE TABLE inventory (id INT PRIMARY KEY, product_name VARCHAR(100), quantity INT, price DOUBLE)"

echo ""
echo -e "${BLUE}[2/4] Inserting OLTP data...${NC}"

# Users — 10 rows
query "INSERT INTO users VALUES (1, 'Alice Johnson', 'alice@techcorp.io', 28)"
query "INSERT INTO users VALUES (2, 'Bob Martinez', 'bob@dataworks.com', 34)"
query "INSERT INTO users VALUES (3, 'Charlie Lee', 'charlie@startup.dev', 22)"
query "INSERT INTO users VALUES (4, 'Diana Patel', 'diana@enterprise.co', 31)"
query "INSERT INTO users VALUES (5, 'Ethan Brown', 'ethan@cloudops.net', 27)"
query "INSERT INTO users VALUES (6, 'Fiona Chen', 'fiona@analytics.io', 29)"
query "INSERT INTO users VALUES (7, 'George Kim', 'george@bigdata.com', 38)"
query "INSERT INTO users VALUES (8, 'Hannah Davis', 'hannah@devstudio.co', 25)"
query "INSERT INTO users VALUES (9, 'Ivan Petrov', 'ivan@sysarch.dev', 42)"
query "INSERT INTO users VALUES (10, 'Julia Morales', 'julia@webscale.io', 33)"

# Orders — 15 rows
query "INSERT INTO orders VALUES (1, 1, 'Widget Pro', 29.99)"
query "INSERT INTO orders VALUES (2, 2, 'Gadget Max', 49.99)"
query "INSERT INTO orders VALUES (3, 1, 'Sensor Kit', 89.50)"
query "INSERT INTO orders VALUES (4, 3, 'Data Logger', 199.00)"
query "INSERT INTO orders VALUES (5, 5, 'Widget Pro', 29.99)"
query "INSERT INTO orders VALUES (6, 4, 'Cloud License', 499.00)"
query "INSERT INTO orders VALUES (7, 6, 'Analytics Suite', 799.00)"
query "INSERT INTO orders VALUES (8, 2, 'Sensor Kit', 89.50)"
query "INSERT INTO orders VALUES (9, 7, 'Data Logger', 199.00)"
query "INSERT INTO orders VALUES (10, 8, 'Widget Pro', 29.99)"
query "INSERT INTO orders VALUES (11, 9, 'Enterprise Pack', 1299.00)"
query "INSERT INTO orders VALUES (12, 10, 'Gadget Max', 49.99)"
query "INSERT INTO orders VALUES (13, 3, 'Cloud License', 499.00)"
query "INSERT INTO orders VALUES (14, 5, 'Analytics Suite', 799.00)"
query "INSERT INTO orders VALUES (15, 1, 'Enterprise Pack', 1299.00)"

# Inventory — 8 rows
query "INSERT INTO inventory VALUES (1, 'Widget Pro', 500, 29.99)"
query "INSERT INTO inventory VALUES (2, 'Gadget Max', 250, 49.99)"
query "INSERT INTO inventory VALUES (3, 'Sensor Kit', 100, 89.50)"
query "INSERT INTO inventory VALUES (4, 'Data Logger', 75, 199.00)"
query "INSERT INTO inventory VALUES (5, 'Cloud License', 9999, 499.00)"
query "INSERT INTO inventory VALUES (6, 'Analytics Suite', 9999, 799.00)"
query "INSERT INTO inventory VALUES (7, 'Enterprise Pack', 50, 1299.00)"
query "INSERT INTO inventory VALUES (8, 'Starter Kit', 1000, 9.99)"

# ═══════════════════════════════════════════════════════════════
# PHASE 2: OLAP TABLE — Large, analytical
# ═══════════════════════════════════════════════════════════════
echo ""
echo -e "${BLUE}[3/4] Creating OLAP table and inserting analytical data (100+ rows)...${NC}"

query "CREATE TABLE sales_events (id INT PRIMARY KEY, region VARCHAR(50), category VARCHAR(50), revenue DOUBLE, units INT)"

# Regions: North, South, East, West, Central
# Categories: Electronics, Software, Hardware, Services, Support
REGIONS=("North" "South" "East" "West" "Central")
CATEGORIES=("Electronics" "Software" "Hardware" "Services" "Support")

id=1
for region in "${REGIONS[@]}"; do
  for category in "${CATEGORIES[@]}"; do
    # 4 entries per region-category combo = 100 rows total
    for j in 1 2 3 4; do
      revenue=$(( (RANDOM % 9000) + 1000 ))
      units=$(( (RANDOM % 200) + 10 ))
      rev_decimal="${revenue}.$(( RANDOM % 100 ))"
      query "INSERT INTO sales_events VALUES ($id, '$region', '$category', $rev_decimal, $units)"
      id=$((id + 1))
    done
  done
done

# ═══════════════════════════════════════════════════════════════
# PHASE 3: PUMP DASHBOARD STATS — reads, range queries
# ═══════════════════════════════════════════════════════════════
echo ""
echo -e "${BLUE}[4/4] Running queries to populate dashboard stats...${NC}"

# Point reads (OLTP)
query "SELECT * FROM users WHERE id = 1"
query "SELECT * FROM users WHERE id = 5"
query "SELECT * FROM users WHERE id = 8"
query "SELECT * FROM orders WHERE id = 3"
query "SELECT * FROM orders WHERE id = 7"
query "SELECT * FROM orders WHERE id = 12"
query "SELECT * FROM inventory WHERE id = 1"
query "SELECT * FROM inventory WHERE id = 4"

# Range/analytical queries (OLAP)
query "SELECT * FROM users"
query "SELECT * FROM orders"
query "SELECT * FROM inventory"
query "SELECT * FROM sales_events"
query "SELECT name, email FROM users WHERE age > 30"
query "SELECT COUNT(*) FROM users"
query "SELECT COUNT(*) FROM orders"
query "SELECT COUNT(*) FROM sales_events"
query "SELECT SUM(amount) FROM orders"
query "SELECT SUM(revenue) FROM sales_events"
query "SELECT AVG(revenue) FROM sales_events"
query "SELECT MIN(revenue) FROM sales_events"
query "SELECT MAX(revenue) FROM sales_events"
query "SELECT SUM(units) FROM sales_events"

# Extra reads to bump stats
for i in $(seq 1 20); do
  query "SELECT * FROM users WHERE id = $(( (RANDOM % 10) + 1 ))"
done

echo ""
echo -e "${BOLD}${GREEN}Done!${NC} Database seeded for demo."
echo ""
echo -e "${CYAN}Tables created:${NC}"
echo "  users         — 10 rows   (OLTP, transactional)"
echo "  orders        — 15 rows   (OLTP, transactional)"
echo "  inventory     —  8 rows   (OLTP, lookup)"
echo "  sales_events  — 100 rows  (OLAP, analytical)"
echo ""
echo -e "${CYAN}Demo queries to try:${NC}"
echo "  OLTP:  SELECT * FROM users WHERE id = 3"
echo "  OLTP:  SELECT name, email FROM users WHERE age > 25"
echo "  OLAP:  SELECT COUNT(*) FROM sales_events"
echo "  OLAP:  SELECT SUM(revenue) FROM sales_events"
echo "  OLAP:  SELECT AVG(revenue) FROM sales_events"
echo ""
