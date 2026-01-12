import psycopg2
import select
import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_CONNECTION = os.environ.get("POSTGRES_CONNECTION")

conn = psycopg2.connect(POSTGRES_CONNECTION)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

cur = conn.cursor()
cur.execute("LISTEN schema_changes;")
print("Waiting for notifications on channel 'schema_changes'...")

while True:
    if select.select([conn], [], [], 5) == ([], [], []):
        continue
    conn.poll()
    while conn.notifies:
        notify = conn.notifies.pop(0)
        print("Got NOTIFY:", notify.payload)
