# Codd Relational Principles — Required Rule Evaluation for Project Samanvay

This assessment focuses on a **practical required subset** of Codd’s rules for a modern HTAP storage engine with SQL support, rather than strict full RDBMS compliance.

## Scope used for this evaluation

- The SQL/API surface currently exposes: `CREATE TABLE`, `DROP TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `SHOW TABLES`, `EXPLAIN`.
- Aggregations supported: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.
- WHERE predicates supported: `=`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`.

## Quick verdict

| Rule (practical subset) | Verdict | Notes |
|---|---|---|
| Rule 0 — Foundation rule | ⚠️ Partial | SQL exists, but relational completeness is limited (no joins/views/relational algebra completeness). |
| Rule 1 — Information rule | ✅ Mostly met | Data is represented as table rows/columns in SQL layer. |
| Rule 2 — Guaranteed access | ✅ Met (single-table scope) | Table + primary key can address rows directly via key encoding and point lookup. |
| Rule 3 — Systematic treatment of nulls | ⚠️ Partial | Null semantics are represented by empty string in parts of SQL path; true 3-valued NULL logic is incomplete. |
| Rule 4 — Dynamic online catalog | ⚠️ Partial | Schema registry exists and persists, but exposed via API/registry calls, not full relational information schema tables. |
| Rule 5 — Comprehensive data sublanguage | ⚠️ Partial | Solid subset of DDL/DML, but no JOIN, GROUP BY, HAVING, ORDER BY, subqueries, etc. |
| Rule 8 — Physical data independence | ✅ Good | LSM + columnar internals are hidden behind StorageEngine/QueryRouter. |
| Rule 10 — Integrity independence | ❌ Not met | Only limited constraints (primary key / not-null-like checks); no declarative FK/CK/unique catalogs independent of app code. |

## Evidence highlights

1. **Relational table abstraction exists in SQL layer** through schema registry, table creation, table listing, and row encoding/decoding.
2. **Guaranteed access is strongest via primary-key addressing** (`table:pk`) and point lookups.
3. **NULL handling is not fully relational** because literals/serialization often collapse null to empty-string behavior.
4. **Comprehensive sublanguage is intentionally limited** to a focused SQL subset suitable for HTAP demo and core paths.
5. **Physical data independence is a strength** due to facade + query router + dual row/columnar paths.
6. **Integrity independence remains the largest gap** (no full declarative constraints, no RI/FK framework).

## Suggested next steps (highest impact)

1. Add first-class NULL representation (distinct from empty string) and 3-valued logic in WHERE evaluation.
2. Introduce system catalog tables (e.g., `information_schema.tables`, `information_schema.columns`) queryable via SQL.
3. Expand SQL coverage incrementally: `JOIN`, `GROUP BY`, `ORDER BY`, projection expression support.
4. Add declarative integrity subsystem: `UNIQUE`, `CHECK`, `FOREIGN KEY` (with enforcement stage controls).
5. Add transaction semantics beyond per-key operations (atomic multi-row commit + rollback isolation model).

## Conclusion

For an HTAP-focused system, Samanvay demonstrates a **strong storage architecture** and a **useful relational subset**, but it is currently best classified as **relationally inspired / partially relational-compliant** rather than Codd-complete.
