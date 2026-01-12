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

# Map specific tables to specific channels
MONITORED_TABLES = {
    "Client": {"channel": "log-new-entries", "display_column": "name", "label": "New Client Added"},
    "Customer": {"channel": "log-new-entries", "display_column": "email", "label": "New User Registered"},
    "SystemWarning": {"channel": "log-new-entries", "display_column": "description", "label": "🚨 SYSTEM WARNING 🚨"}
}

# Global Deduplication State
_notification_lock = threading.Lock()
_recent_notifications = {}

# Helper to preventing Slack 3000 char crashes
def safe_truncate(text, limit=2500):
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n\n... [Truncated: Content exceeded Slack limit of {limit} chars]"

# ------------------------------------------------------------
# 2. Slack Interactions (Modals)
# ------------------------------------------------------------

@app.action("view_schema_details")
def handle_view_schema(ack, body, client):
    ack()
    raw_identity = body['actions'][0]['value']
    
    # Handle Quoting (e.g. public.Response -> "public"."Response")
    if "." in raw_identity:
        s_name, t_name = raw_identity.split(".", 1)
        quoted_identity = f'"{s_name}"."{t_name}"'
    else:
        s_name, t_name = "public", raw_identity
        quoted_identity = f'"{t_name}"'

    try:
        conn = psycopg2.connect(POSTGRES_URL)
        with conn.cursor() as cur:
            # Query to reconstruct the CREATE TABLE statement
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
            sql_def = res[0] if res else "-- Definition not found. The table may have been deleted."
        conn.close()
    except Exception as e:
        sql_def = f"-- Error generating SQL: {str(e)}"

    # SAFETY: Truncate SQL if it's too long for Slack
    safe_sql = safe_truncate(sql_def, limit=2800)

    client.views_open(trigger_id=body["trigger_id"], view={
        "type": "modal",
        "title": {"type": "plain_text", "text": "Table Definition"},
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"SQL for `{raw_identity}`:"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"```sql\n{safe_sql}\n```"}}
        ],
        "close": {"type": "plain_text", "text": "Close"}
    })

@app.action("view_row_details")
def handle_view_row(ack, body, client):
    ack()
    row_data = json.loads(body['actions'][0]['value'])
    formatted = "\n".join([f"• *{k}*: `{v}`" for k, v in row_data.items()])
    
    # SAFETY: Truncate row data if it's too massive
    safe_data = safe_truncate(formatted, limit=2800)

    client.views_open(trigger_id=body["trigger_id"], view={
        "type": "modal",
        "title": {"type": "plain_text", "text": "Row Details"},
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": "Complete row data:"}},
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": safe_data}}
        ],
        "close": {"type": "plain_text", "text": "Close"}
    })

# ------------------------------------------------------------
# 3. Notification Logic
# ------------------------------------------------------------

def handle_schema_notification(payload):
    print(f"📥 RAW PAYLOAD: {payload}")

    command = payload.get("command_tag", "")
    # Try multiple keys to get the name
    raw_name = payload.get("object_name") or payload.get("object_identity") or ""
    obj_type = payload.get("object_type") 

    # --- 1. Basic Validation ---
    if not raw_name or command not in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE"):
        return

    # --- 2. PRE-PROCESS & NORMALIZE NAME ---
    # This turns 'public.schema_test_modal[]' into 'schema_test_modal'
    # so the deduplicator knows they are the same thing.
    normalized_name = raw_name.replace('public.', '').replace('"', '').replace('[]', '').strip()

    # --- 3. NOISE FILTER ---
    ignored_patterns = ["_pkey", "_seq", "_idx", "pg_toast", "_fkey", "unique", "exclusion"]
    if any(pattern in normalized_name.lower() for pattern in ignored_patterns):
        print(f"🚫 IGNORED (Pattern Match): {normalized_name}")
        return

    # --- 4. TYPE FILTER ---
    if obj_type and obj_type != 'table':
        print(f"🚫 IGNORED (Type Mismatch): {normalized_name} is type {obj_type}")
        return

    # --- 5. DEDUPLICATION (using the normalized name) ---
    now = datetime.now()
    with _notification_lock:
        if normalized_name in _recent_notifications:
            last_seen = _recent_notifications[normalized_name]
            if now - last_seen < timedelta(seconds=5):
                print(f"🚫 IGNORED (Duplicate/Too Soon): {normalized_name}")
                return
        _recent_notifications[normalized_name] = now

    # --- 6. SEND TO SLACK ---
    is_drop = "DROP" in command
    
    blocks = [{
        "type": "section",
        "text": {
            "type": "mrkdwn", 
            "text": f"{'🗑️ *Table Deleted*' if is_drop else '📝 *Schema Change Detected*'}\n• *Table:* `{normalized_name}`\n• *Action:* `{command}`"
        }
    }]

    if not is_drop:
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "🔍 View Full Schema"},
                "value": normalized_name,
                "action_id": "view_schema_details"
            }]
        })

    try:
        app.client.chat_postMessage(channel=DEFAULT_ALERTS_CHANNEL, blocks=blocks, text=f"Schema Update: {normalized_name}")
        print(f"✅ SENT SLACK MSG: {normalized_name}")
    except Exception as e:
        print(f"❌ SLACK ERROR: {e}")

def handle_data_notification(payload):
    table_name = payload.get("table")
    row_data = payload.get("data", {})

    if table_name in MONITORED_TABLES:
        config = MONITORED_TABLES[table_name]
        display_val = row_data.get(config["display_column"], "N/A")
        
        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"✨ *{config['label']}*\n• *Item:* `{display_val}`\n• *Table:* `{table_name}`"}},
            {"type": "actions", "elements": [{
                "type": "button", 
                "text": {"type": "plain_text", "text": "🔍 View Full Entry"},
                "value": json.dumps(row_data),
                "action_id": "view_row_details"
            }]}
        ]
        try:
            app.client.chat_postMessage(channel=config["channel"], blocks=blocks, text=f"New entry in {table_name}")
        except Exception as e:
            print(f"Slack Data Error: {e}")

# ------------------------------------------------------------
# 4. Main Listener Loop
# ------------------------------------------------------------

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
                        if notify.channel == 'schema_changes':
                            handle_schema_notification(data)
                        elif notify.channel == 'table_data_updates':
                            handle_data_notification(data)
                time.sleep(1)
        except Exception as e:
            print(f"⚠️ DB Connection lost: {e}. Retrying in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    threading.Thread(target=listen_forever, daemon=True).start()
    handler.start()