CREATE SCHEMA IF NOT EXISTS {};
"""

dim_all_calls = """

CREATE TABLE IF NOT EXISTS {}.dim_all_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
agent_id INTEGER,
call_type VARCHAR,
assigned_agent_id INTEGER,
call_duration_in_seconds INTEGER,
call_ended_by_agent BOOLEAN

);
"""
dim_recieved_calls = """
CREATE TABLE IF NOT EXISTS {}.dim_recieved_calls(
id INTEGER,
recieved_call_id INTEGER,
inbound_caller_id VARCHAR,
call_duration_in_seconds INTEGER,
recieving_agent_id INTEGER,
assigned_agent_id INTEGER

);
"""
dim_assigned_calls = """
CREATE TABLE IF NOT EXISTS {}.dim_assigned_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
assigned_calls_to_agent_id INTEGER,
complaint_status VARCHAR
);
"""
dim_resolved_calls = """
CREATE TABLE IF NOT EXISTS {}.dim_resolved_calls(
id INTEGER,
resolved_call_id INTEGER,
caller_id VARCHAR,
recieving_agent_id INTEGER,
assigned_agent_id INTEGER,
complaint_status VARCHAR,
resolution_duration_in_hours INTEGER
);
"""

dim_complaints = """
CREATE TABLE IF NOT EXISTS {}.dim_complaints(
id INTEGER,
call_id INTEGER,
complaint_topic VARCHAR,
complaint_status VARCHAR

);
"""
dim_agents = """
CREATE TABLE IF NOT EXISTS {}.dim_agents(
id INTEGER,
agent_id INTEGER,
agents_grade_level VARCHAR

);
"""

fact_call_resolution = """
CREATE TABLE IF NOT EXISTS {}.fact_call_resolution(
all_calls_id INTEGER,
all_calls_call_id INTEGER,
all_callers_id VARCHAR,
assigned_id INTEGER,
assigned_call_id INTEGER,
resolved_id INTEGER,
resolved_calls_id INTEGER,
complaint_id INTEGER,
resolved_agent_id INTEGER,
recieved_call_id INTEGER,
agent_id INTEGER,
resolution_duration_in_hours INTEGER,
call_duration_in_seconds INTEGER

)
"""

transform_tables = [dim_all_calls, dim_recieved_calls, dim_assigned_calls,
                            dim_resolved_calls, dim_complaints, dim_agents, fact_call_resolution]


dim_all_calls = """
INSERT INTO {}.dim_all_calls(
id,
call_id,
caller_id,
agent_id,
call_type,
assigned_agent_id,
call_duration_in_seconds,
call_ended_by_agent
)
SELECT
a.id,
a.call_id,
a.caller_id,
a.recieving_agent_id as agent_id,
a.call_type,
a.assigned_agent_id,
a.call_duration_in_seconds,
a.call_ended_by_agent
FROM raw_schema.all_calls a

"""

dim_recieved_calls = """
INSERT INTO {}.dim_recieved_calls(
id,
recieved_call_id,
inbound_caller_id,
call_duration_in_seconds,
recieving_agent_id,
assigned_agent_id
)
SELECT
r.id,
r.call_id as recieved_call_id,
r.caller_id as inbound_caller_id,
r.call_duration_in_seconds,
r.recieving_agent_id,
r.assigned_agent_id
FROM raw_schema.recieved_calls r

"""

dim_resolved_calls = """
INSERT INTO {}.dim_resolved_calls(
id,
resolved_call_id,
caller_id,
recieving_agent_id,
assigned_agent_id,
complaint_status,
resolution_duration_in_hours
)
SELECT
re.id,
re.call_id as resolved_call_id,
re.caller_id,
re.recieving_agent_id,
re.assigned_agent_id,
re.complaint_status,
re.resolution_duration_in_hours
FROM raw_schema.resolved_calls re

"""
dim_assigned_calls = """
INSERT INTO {}.dim_assigned_calls(
id,
call_id,
caller_id,
assigned_calls_to_agent_id,
complaint_status
)
SELECT
a.id,
a.call_id,
caller_id,
a.assigned_calls_to_agent_id,
a.complaint_status
FROM raw_schema.assigned_calls a

"""

dim_complaints = """
INSERT INTO {}.dim_complaints(
id,
call_id,
complaint_topic,
complaint_status
)
SELECT
c.id,
c.call_id,
c.complaint_topic,
c.complaint_status
FROM raw_schema.complaints c

"""
dim_agents = """
INSERT INTO {}.dim_agents(
id,
agent_id,
agents_grade_level
)
SELECT
da.id,
da.recieving_agent_id as agent_id,
da.agents_grade_level
FROM raw_schema.agents da

"""

fact_call_resolution = """
INSERT INTO {}.fact_call_resolution(
all_calls_id,
all_calls_call_id,
all_callers_id,
assigned_id,
assigned_call_id,
resolved_id,
resolved_calls_id,
complaint_id,
recieved_call_id,
resolved_agent_id,
agent_id,
resolution_duration_in_hours,
call_duration_in_seconds
)
SELECT
a.id as all_calls_id,
a.call_id as all_calls_call_id,
a.caller_id as all_callers_id,
asi.id,
asi.call_id,
res.id as resolved_id,
res.call_id as resolved_calls_id,
c.id as complaint_id,
rec.call_id as recieved_call_id,
res.assigned_agent_id as resolved_agent_id,
ag.recieving_agent_id,
res.resolution_duration_in_hours,
a.call_duration_in_seconds
FROM raw_schema.all_calls a
LEFT JOIN raw_schema.assigned_calls asi
ON
a.call_id = asi.call_id
LEFT JOIN raw_schema.resolved_calls res
ON
a.call_id = res.call_id
LEFT JOIN raw_schema.complaints c
ON
a.call_id = c.call_id
LEFT JOIN raw_schema.recieved_calls rec
ON 
a.call_id = rec.call_id
LEFT JOIN raw_schema.agents ag
ON
a.call_id = ag.call_id
"""

insert_into_transform = [dim_all_calls, dim_recieved_calls,
                                 dim_resolved_calls, dim_assigned_calls, dim_complaints, dim_agents, fact_call_resolution]
