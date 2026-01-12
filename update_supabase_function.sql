-- Update the Supabase function to include table name and object type
-- Run this in your Supabase SQL Editor

CREATE OR REPLACE FUNCTION notify_schema_change()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        PERFORM pg_notify(
            'schema_changes',
            json_build_object(
                'timestamp', now(),
                'username', session_user,
                'command_tag', TG_TAG,
                'object_type', obj.object_type,
                'object_name', obj.object_identity
            )::text
        );
    END LOOP;
END;
$$;

