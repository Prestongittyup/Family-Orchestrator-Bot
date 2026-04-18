import sqlite3

conn = sqlite3.connect('data/family_orchestration.db')
cursor = conn.cursor()

print('Event Logs with Idempotency Keys:')
print('=' * 140)
cursor.execute('SELECT id, type, household_id, idempotency_key, created_at FROM event_logs WHERE type = "email_received" ORDER BY created_at DESC LIMIT 5')
rows = cursor.fetchall()

for row in rows:
    print(f'ID: {row[0][:8]}... | Type: {row[1]:<15} | Household: {row[2]:<10} | Idempotency Key: {row[3]}')

print()
total_with_key = cursor.execute('SELECT COUNT(*) FROM event_logs WHERE idempotency_key IS NOT NULL').fetchone()[0]
total_events = cursor.execute('SELECT COUNT(*) FROM event_logs').fetchone()[0]
print(f'Events with idempotency_key: {total_with_key} / {total_events}')

conn.close()
