import sqlite3

conn = sqlite3.connect('data/family_orchestration.db')
cursor = conn.cursor()

print('Tasks Table (Verify Email->Task Conversion):')
print('=' * 120)
cursor.execute('SELECT id, household_id, title, status, priority, description, created_at FROM tasks ORDER BY created_at DESC LIMIT 5')
rows = cursor.fetchall()

for row in rows:
    print(f'Task ID: {row[0][:8]}...')
    print(f'  Household: {row[1]}, Title: {row[2]:<30} Status: {row[3]:<12} Priority: {row[4]}')
    print(f'  Description: {row[5]}')
    print(f'  Created: {row[6]}')
    print()

total_tasks = cursor.execute('SELECT COUNT(*) FROM tasks').fetchone()[0]
print(f'Total tasks in database: {total_tasks}')

print()
print('Event Logs Summary:')
print('=' * 120)
total_events = cursor.execute('SELECT COUNT(*) FROM event_logs').fetchone()[0]
print(f'Total events in event_logs: {total_events}')

cursor.execute('SELECT type, COUNT(*) FROM event_logs GROUP BY type')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

conn.close()
