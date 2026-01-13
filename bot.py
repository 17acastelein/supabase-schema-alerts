import os
import json
import psycopg2
import psycopg2.extensions
import threading
import time
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ------------------------------------------------------------
# 1. Initialization & Configuration
# ------------------------------------------------------------
load_dotenv()

app = App(token=os.getenv("SLACK_BOT_TOKEN"))
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
POSTGRES_URL = os.getenv("POSTGRES_URL")

DEFAULT_ALERTS_CHANNEL = os.getenv("SLACK_CHANNEL", "supabase-schema-updates")

MONITORED_TABLES = {
    "Client": {"channel": "log-new-entries", "display_column": "name", "label": "New Client Added"},
    "Customer": {"channel": "log-new-entries", "display_column": "email", "label": "New User Registered"},
    "SystemWarning": {"channel": "log-new-entries", "display_column": "description", "label": "🚨 SYSTEM WARNING 🚨"}
}

_notification_lock = threading.Lock()
_recent_notifications = {}

def safe_truncate(text, limit=2500):
    if not text: return ""
    return text if len(text) <= limit else text[:limit] + "\n\n... [Truncated: Exceeded Slack Limits]"

# ------------------------------------------------------------
# 2. Database & Snapshot Logic
# ------------------------------------------------------------

def get_current_table_schema(conn, full_table_name):
    """Fetches structured JSON and raw SQL for a single table."""
    if "." in full_table_name:
        s_name, t_name = full_table_name.split(".", 1)
        quoted_identity = f'"{s_name}"."{t_name}"'
    else:
        s_name, t_name = "public", full_table_name
        quoted_identity = f'"{t_name}"'

    with conn.cursor() as cur:
        # 1. Get Columns as JSON
        cur.execute("""
            SELECT jsonb_object_agg(column_name, json_build_object(
                'type', data_type,
                'nullable', is_nullable,
                'default', column_default
            ))
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
        """, (s_name, t_name))
        res = cur.fetchone()
        cols_json = res[0] if res else {}

        # 2. Get Raw SQL Definition
        cur.execute("""
            WITH cols AS (
                SELECT column_name, data_type, column_default, is_nullable, ordinal_position
                FROM information_schema.columns WHERE table_schema = %s AND table_name = %s
            ),
            constraints AS (
                SELECT pg_get_constraintdef(oid) as condef FROM pg_constraint WHERE conrelid = %s::regclass
            ),
            indexes AS (
                SELECT indexdef FROM pg_indexes WHERE schemaname = %s AND tablename = %s
            ),
            triggers AS (
                SELECT pg_get_triggerdef(oid) as trigdef FROM pg_trigger WHERE tgrelid = %s::regclass AND NOT tgisinternal
            )
            SELECT 
                'CREATE TABLE ' || %s || ' (' || chr(10) ||
                (SELECT string_agg('  ' || column_name || ' ' || data_type || 
                    CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END ||
                    CASE WHEN column_default IS NOT NULL THEN ' DEFAULT ' || column_default ELSE '' END, 
                    ',' || chr(10) ORDER BY ordinal_position) FROM cols) ||
                COALESCE((SELECT chr(10) || '  ' || string_agg(condef, ',' || chr(10) || '  ') FROM constraints), '') ||
                chr(10) || ');' || chr(10) || chr(10) ||
                COALESCE((SELECT string_agg(indexdef, ';' || chr(10)) || ';' FROM indexes), '') || chr(10) || chr(10) ||
                COALESCE((SELECT string_agg(trigdef, ';' || chr(10)) || ';' FROM triggers), '')
        """, (s_name, t_name, quoted_identity, s_name, t_name, quoted_identity, quoted_identity))
        
        res = cur.fetchone()
        raw_sql_string = res[0] if res else ""
        
        return cols_json, raw_sql_string

def sync_schema_snapshots():
    """Runs on startup. Prunes ghosts, upserts reality."""
    print("🔄 Syncing schema snapshots...")
    try:
        conn = psycopg2.connect(POSTGRES_URL)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name != 'schema_snapshots'
            """)
            live_tables = [row[0] for row in cur.fetchall()]
            live_full_names = [f"public.{t}" for t in live_tables]

            if live_full_names:
                cur.execute("DELETE FROM public.schema_snapshots WHERE table_name NOT IN %s", (tuple(live_full_names),))
            else:
                cur.execute("DELETE FROM public.schema_snapshots")

            for t in live_tables:
                full_name = f"public.{t}"
                cols, sql = get_current_table_schema(conn, full_name)
                cur.execute("""
                    INSERT INTO public.schema_snapshots (table_name, columns_data, raw_sql, updated_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (table_name) DO UPDATE SET columns_data = EXCLUDED.columns_data, raw_sql = EXCLUDED.raw_sql, updated_at = now()
                """, (full_name, json.dumps(cols), sql))
            
            conn.commit()
            print(f"✅ Synced {len(live_tables)} tables.")
        conn.close()
    except Exception as e:
        print(f"❌ Startup Sync Error: {e}")

# ------------------------------------------------------------
# 3. Diff Logic (Visual Alterations)
# ------------------------------------------------------------

def generate_schema_diff(old_cols, new_cols):
    """Compares two JSON column objects and returns a formatted string."""
    changes = []
    
    # Check for Additions
    for col, details in new_cols.items():
        if col not in old_cols:
            dtype = details.get('type', 'unknown')
            changes.append(f"🟢 *Added Column:* `{col}` ({dtype})")
    
    # Check for Removals
    for col, details in old_cols.items():
        if col not in new_cols:
            changes.append(f"🔴 *Removed Column:* `{col}`")

    # Check for Type Changes
    for col, new_details in new_cols.items():
        if col in old_cols:
            old_details = old_cols[col]
            if old_details.get('type') != new_details.get('type'):
                changes.append(f"🔵 *Modified Type:* `{col}` ({old_details.get('type')} → {new_details.get('type')})")

    if not changes:
        return "• _No column structure changes detected (might be a constraint/index change)_"
    
    return "\n".join(changes)

# ------------------------------------------------------------
# 4. Slack & Notification Logic
# ------------------------------------------------------------

@app.action("view_schema_details")
def handle_view_schema(ack, body, client):
    ack()
    raw_identity = body['actions'][0]['value']
    try:
        conn = psycopg2.connect(POSTGRES_URL)
        _, sql_def = get_current_table_schema(conn, raw_identity)
        conn.close()
    except Exception as e:
        sql_def = f"-- Error retrieving schema: {e}"

    client.views_open(trigger_id=body["trigger_id"], view={
        "type": "modal", "title": {"type": "plain_text", "text": "Table Definition"},
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"SQL for `{raw_identity}`:"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"```sql\n{safe_truncate(sql_def)}\n```"}}
        ],
        "close": {"type": "plain_text", "text": "Close"}
    })

def handle_schema_notification(payload):
    print(f"📥 RAW: {payload}")
    command = payload.get("command_tag", "")
    raw_name = payload.get("object_name") or payload.get("object_identity") or ""
    
    if not raw_name or command not in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE"): return

    normalized_name = raw_name.replace('public.', '').replace('"', '').replace('[]', '').strip()
    full_db_name = f"public.{normalized_name}"
    
    if any(p in normalized_name.lower() for p in ["_pkey", "_seq", "_idx", "pg_toast", "_fkey"]): return

    now = datetime.now()
    with _notification_lock:
        if normalized_name in _recent_notifications:
            if now - _recent_notifications[normalized_name] < timedelta(seconds=5): return
        _recent_notifications[normalized_name] = now

    diff_text = ""
    
    # --- LOGIC BRANCHING ---
    conn = psycopg2.connect(POSTGRES_URL)
    
    try:
        # CASE 1: DROP TABLE (Cleanup)
        if "DROP" in command:
            print(f"🧹 Deleting snapshot for {full_db_name}")
            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.schema_snapshots WHERE table_name = %s", (full_db_name,))
            conn.commit()

        # CASE 2: ALTER TABLE (Compare -> Notify -> Update)
        elif "ALTER" in command:
            with conn.cursor() as cur:
                # 1. Fetch OLD state
                cur.execute("SELECT columns_data FROM public.schema_snapshots WHERE table_name = %s", (full_db_name,))
                res = cur.fetchone()
                old_cols = res[0] if res else {}

                # 2. Fetch NEW state
                new_cols, new_sql = get_current_table_schema(conn, full_db_name)

                # 3. Generate Diff
                if old_cols:
                    diff_text = generate_schema_diff(old_cols, new_cols)
                
                # 4. Update Snapshot
                cur.execute("""
                    INSERT INTO public.schema_snapshots (table_name, columns_data, raw_sql, updated_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (table_name) DO UPDATE SET columns_data = EXCLUDED.columns_data, raw_sql = EXCLUDED.raw_sql, updated_at = now()
                """, (full_db_name, json.dumps(new_cols), new_sql))
            conn.commit()

        # CASE 3: CREATE TABLE (Insert Snapshot)
        elif "CREATE" in command:
            # We delay slightly to ensure PG catalog is ready, then sync just this table
            time.sleep(1) 
            with conn.cursor() as cur:
                new_cols, new_sql = get_current_table_schema(conn, full_db_name)
                cur.execute("""
                    INSERT INTO public.schema_snapshots (table_name, columns_data, raw_sql, updated_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (table_name) DO UPDATE SET columns_data = EXCLUDED.columns_data, raw_sql = EXCLUDED.raw_sql, updated_at = now()
                """, (full_db_name, json.dumps(new_cols), new_sql))
            conn.commit()

    except Exception as e:
        print(f"❌ DB Ops Error: {e}")
    finally:
        conn.close()

    # --- SLACK MESSAGE BUILDING ---
    is_drop = "DROP" in command
    
    # Base Message
    main_text = f"{'🗑️ *Table Deleted*' if is_drop else '📝 *Schema Change Detected*'}\n• *Table:* `{normalized_name}`\n• *Action:* `{command}`"
    
    # If we have a diff (only happens on ALTER), append it
    if diff_text:
        main_text += f"\n\n*Changes Detected:*\n{diff_text}"

    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": main_text}}]
    
    if not is_drop:
        blocks.append({"type": "actions", "elements": [{"type": "button", "text": {"type": "plain_text", "text": "🔍 View Full Schema"}, "value": normalized_name, "action_id": "view_schema_details"}]})

    try:
        app.client.chat_postMessage(channel=DEFAULT_ALERTS_CHANNEL, blocks=blocks, text=f"Schema Update: {normalized_name}")
        print(f"✅ Alert Sent: {normalized_name}")
    except Exception as e:
        print(f"❌ Slack Error: {e}")

# ------------------------------------------------------------
# 5. Data & Main Loop
# ------------------------------------------------------------

def handle_data_notification(payload):
    table_name = payload.get("table")
    row_data = payload.get("data", {})
    if table_name in MONITORED_TABLES:
        config = MONITORED_TABLES[table_name]
        display_val = row_data.get(config["display_column"], "N/A")
        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"✨ *{config['label']}*\n• *Item:* `{display_val}`\n• *Table:* `{table_name}`"}},
            {"type": "actions", "elements": [{"type": "button", "text": {"type": "plain_text", "text": "🔍 View Full Entry"}, "value": json.dumps(row_data), "action_id": "view_row_details"}]}
        ]
        try:
            app.client.chat_postMessage(channel=config["channel"], blocks=blocks, text=f"New entry in {table_name}")
        except Exception as e:
            print(f"Slack Data Error: {e}")

@app.action("view_row_details")
def handle_view_row(ack, body, client):
    ack()
    row_data = json.loads(body['actions'][0]['value'])
    formatted = "\n".join([f"• *{k}*: `{v}`" for k, v in row_data.items()])
    client.views_open(trigger_id=body["trigger_id"], view={
        "type": "modal", "title": {"type": "plain_text", "text": "Row Details"},
        "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Complete row data:"}}, {"type": "divider"}, {"type": "section", "text": {"type": "mrkdwn", "text": safe_truncate(formatted)}}],
        "close": {"type": "plain_text", "text": "Close"}
    })

def listen_forever():
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_URL)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute("LISTEN schema_changes;")
            cur.execute("LISTEN table_data_updates;")
            print("🚀 Bot is live and listening for Postgres events...")
            while True:
                if conn.poll() == psycopg2.extensions.POLL_OK:
                    while conn.notifies:
                        notify = conn.notifies.pop(0)
                        data = json.loads(notify.payload)
                        if notify.channel == 'schema_changes': handle_schema_notification(data)
                        elif notify.channel == 'table_data_updates': handle_data_notification(data)
                time.sleep(1)
        except Exception as e:
            print(f"⚠️ DB Connection lost: {e}. Retrying in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    sync_schema_snapshots()
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    threading.Thread(target=listen_forever, daemon=True).start()
    handler.start()