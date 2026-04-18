import sqlite3

conn = sqlite3.connect('data.db')
cursor = conn.cursor()

# Get event logs
cursor.execute('SELECT id, type, household_id, severity, created_at FROM event_logs ORDER BY created_at DESC LIMIT 5')
rows = cursor.fetchall()

print('Event Logs Table (SQL):')
print('-' * 100)
for row in rows:
    print(f'ID: {row[0][:8]}... | Type: {row[1]:<15} | Household: {row[2]:<10} | Severity: {row[3]:<6} | Created: {row[4]}')

print()
print(f'Total records in event_logs: {cursor.execute("SELECT COUNT(*) FROM event_logs").fetchone()[0]}')

# Get full event details for latest
print()
print('Latest Event Details (Full):')
print('-' * 100)
cursor.execute('SELECT id, type, household_id, payload, severity, created_at FROM event_logs ORDER BY created_at DESC LIMIT 1')
row = cursor.fetchone()
if row:
    print(f'ID: {row[0]}')
    print(f'Type: {row[1]}')
    print(f'Household: {row[2]}')
    print(f'Payload: {row[3]}')
    print(f'Severity: {row[4]}')
    print(f'Created: {row[5]}')

conn.close()
