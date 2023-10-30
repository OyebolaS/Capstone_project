CREATE SCHEMA IF NOT EXISTS weserve;

--tables
CREATE TABLE IF NOT EXISTS agents(
agent_id INTEGER,
call_id INTEGER,
call_type VARCHAR,
call_duration_in_secs INTEGER,
agent_grade_level VARCHAR
);

CREATE TABLE IF NOT EXISTS calls_made(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
agent_id INTEGER,
assigned_agent_id INTEGER,
call_duration_in_secs INTEGER,
call_type VARCHAR,
call_ended_by_agent BOOLEAN);

CREATE TABLE IF NOT EXISTS calls_received(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
call_type VARCHAR,
complaint_topic VARCHAR,
call_duration_in_secs INTEGER,
agent_id INTEGER,
assigned_agent_id INTEGER
);
 
CREATE TABLE IF NOT EXISTS resolved_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
agent_id INTEGER,
complaint_topic VARCHAR,
complaint_status VARCHAR,
assigned_agent_id INTEGER,
resolution_duration_in_hours INTEGER
);

CREATE TABLE IF NOT EXISTS assigned_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
calls_assigned_to_agent_id INTEGER,
complaint_status VARCHAR
);

CREATE TABLE IF NOT EXISTS complaints(
id INTEGER,
call_id INTEGER,
complaint_topic VARCHAR,
complaint_status VARCHAR
);
