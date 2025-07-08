# reproducer_pg_limit_bind_bug.py
# Reproducer for potential bug in QuestDB PG wire protocol using bind variables in LIMIT and IN clauses

import psycopg2

# Connect to QuestDB via PostgreSQL wire protocol (port 8812)
conn = psycopg2.connect(
    dbname='qdb',
    user='admin',
    password='quest',
    host='localhost',
    port=8812
)

cur = conn.cursor()

# Create test table
cur.execute("""
    CREATE TABLE IF NOT EXISTS test_events (
        id INT,
        name TEXT,
        facility TEXT
    )
""")

# Insert test data
cur.execute("DELETE FROM test_events")  # Clear previous data
cur.execute("INSERT INTO test_events (id, name, facility) VALUES (1, 'alpha', 'core')")
cur.execute("INSERT INTO test_events (id, name, facility) VALUES (2, 'beta', 'edge')")
cur.execute("INSERT INTO test_events (id, name, facility) VALUES (3, 'gamma', 'core')")
conn.commit()

# Test 1: LIMIT with bind parameter
print("\n=== LIMIT bind test ===")
try:
    cur.execute("SELECT * FROM test_events LIMIT %s", (2,))
    rows = cur.fetchall()
    print("✅ LIMIT bind test passed. Rows returned:")
    for row in rows:
        print(row)
except Exception as e:
    print("❌ LIMIT bind test failed with error:", e)

# Test 2: LIMIT + OFFSET
print("\n=== LIMIT + OFFSET bind test ===")
try:
    cur.execute("SELECT * FROM test_events LIMIT %s OFFSET %s", (1, 1))
    rows = cur.fetchall()
    print("✅ LIMIT + OFFSET bind test passed. Rows returned:")
    for row in rows:
        print(row)
except Exception as e:
    print("❌ LIMIT + OFFSET bind test failed with error:", e)

# Test 3: IN clause with multiple binds
print("\n=== IN clause bind test ===")
try:
    cur.execute("SELECT * FROM test_events WHERE name IN (%s, %s)", ('alpha', 'gamma'))
    rows = cur.fetchall()
    print("✅ IN clause bind test passed. Rows returned:")
    for row in rows:
        print(row)
except Exception as e:
    print("❌ IN clause bind test failed with error:", e)

cur.close()
conn.close()