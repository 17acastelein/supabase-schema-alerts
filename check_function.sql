-- Check the current notify_schema_change function definition
SELECT pg_get_functiondef(oid) 
FROM pg_proc 
WHERE proname = 'notify_schema_change';

