import os
import json
import psycopg2
import psycopg2.extensions
import threading
import time
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv

# ------------------------------------------------------------
# Load Environment
# ------------------------------------------------------------
load_dotenv()

app = App(token=os.getenv("SLACK_BOT_TOKEN"))
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
POSTGRES_URL = os.getenv("POSTGRES_URL")

# --- CONFIGURATION ---
# Default channel for schema/system alerts
DEFAULT_ALERTS_CHANNEL = os.getenv("SLACK_CHANNEL", "supabase-schema-updates")

# Map specific tables to specific channels and columns
# Key: Table Name | Value: { Slack Channel ID, Column to show, Label }
MONITORED_TABLES = {
    "Client": {
        "channel": "log-new-entries",  # slack channel id
        "display_column": "name", # data table column to show in the message
        "label": "New Module Added" # message label
    },
    "Customer": {
        "channel": "log-new-entries", 
        "display_column": "email",
        "label": "New User Registered"
    },
    "SystemWarning": {
        "channel": "log-new-entries",  
        "display_column": "description",
        "label": "🚨🚨🚨 SYSTEM WARNING 🚨🚨🚨"
    }
}

# ------------------------------------------------------------
# Notification Handlers
# ------------------------------------------------------------

# Track recently processed notifications to prevent duplicates
_recent_notifications = set()
_notification_lock = threading.Lock()

def handle_schema_notification(conn, payload):
    """Handles CREATE, ALTER, DROP TABLE events."""
    command = payload.get("command_tag", "")
    raw_name = payload.get("object_identity") or payload.get("object_name")
    
    # Filter for table-related DDL only
    if command not in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE"):
        return
    
    if not raw_name:
        return
    
    # Normalize table name: remove quotes and strip PostgreSQL array notation []
    clean_name = raw_name.replace('"', '').rstrip('[]')
    
    # Parse schema.table format
    if "." in clean_name:
        schema_name, table_name = clean_name.split(".", 1)
    else:
        schema_name = "public"
        table_name = clean_name
    
    # Create a unique key for deduplication
    notification_key = (command, schema_name, table_name)
    
    # Check if we've already processed this notification
    with _notification_lock:
        if notification_key in _recent_notifications:
            return  # Skip duplicate
        _recent_notifications.add(notification_key)
        # Keep only the last 100 notifications to prevent memory growth
        if len(_recent_notifications) > 100:
            _recent_notifications.clear()
            _recent_notifications.add(notification_key)
    
    # Format message based on command type
    if "DROP" in command:
        msg = f"🗑️ *Table Deleted*\n• *Table:* `{table_name}`\n• *Action:* `{command}`"
    else:
        msg = f"📝 *Schema Change Detected*\n• *Table:* `{table_name}`\n• *Action:* `{command}`"
    
    try:
        app.client.chat_postMessage(channel=DEFAULT_ALERTS_CHANNEL, text=msg)
    except Exception as e:
        print(f"Slack Error (Schema): {e}")

def handle_data_notification(payload):
    """Handles row-level INSERT events."""
    table_name = payload.get("table")
    row_data = payload.get("data", {})

    # Check if we have a specific rule for this table
    if table_name in MONITORED_TABLES:
        config = MONITORED_TABLES[table_name]
        identifier = row_data.get(config["display_column"], "N/A")
        
        message = (
            f"✨ *{config['label']}*\n"
            f"• *Item:* `{identifier}`\n"
            f"• *Table:* `{table_name}`"
        )
        
        try:
            app.client.chat_postMessage(channel=config["channel"], text=message)
        except Exception as e:
            print(f"Slack Error (Data): {e}")

# ------------------------------------------------------------
# Main Database Listener
# ------------------------------------------------------------

def listen_forever():
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_URL)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            
            # Listen to both types of triggers
            cur.execute("LISTEN schema_changes;")       # From your Event Trigger
            cur.execute("LISTEN table_data_updates;")   # From your Row Triggers
            
            print("🚀 Bot is live. Monitoring all tables for changes and data...")

            while True:
                if conn.poll() == psycopg2.extensions.POLL_OK:
                    while conn.notifies:
                        notify = conn.notifies.pop(0)
                        payload = json.loads(notify.payload)
                        
                        if notify.channel == 'schema_changes':
                            handle_schema_notification(conn, payload)
                        elif notify.channel == 'table_data_updates':
                            handle_data_notification(payload)
                            
                time.sleep(1)
        except Exception as e:
            print(f"Connection lost: {e}. Retrying in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    # Start Slack Socket Mode in background
    threading.Thread(target=lambda: SocketModeHandler(app, SLACK_APP_TOKEN).start(), daemon=True).start()
    # Start Postgres Listener in main thread
    listen_forever()