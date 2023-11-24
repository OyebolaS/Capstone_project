import psycopg2
import csv
import pandas as pd

transform_tables = ['dim_all_calls', 'dim_recieved_calls', 'dim_assigned_calls', 'dim_resolved_calls',  'dim_complaints',  'dim_agents', 'dim_call_resolution']
call_details= pd.read_csv(r'cleaned_call_details.csv')
call_log= pd.read_csv(r'cleaned_call_log.csv')
try:
    conn= psycopg2.connect(
        host= 'localhost',
        user= 'postgres',
        password ='Luvkadend',
        database ='project'
    )

    cursor=conn.cursor()

except Exception as e:
    print(e)
else:
    cursor.execute ("""
CREATE SCHEMA IF NOT EXISTS staged_schema;
CREATE TABLE IF NOT EXISTS dim_calls_made(
id INTEGER,
call_id INTEGER,
caller_id  VARCHAR,
agent_id INTEGER,
call_type VARCHAR,
assigned_to_id INTEGER,
call_duration_in_secs INTEGER,
call_ended_by_agent BOOLEAN);

CREATE TABLE IF NOT EXISTS dim_recieved_calls(
id INTEGER,
received_call_id INTEGER,
inbound_caller_id VARCHAR,
call_duration_in_secs INTEGER,
agent_id INTEGER,
assigned_to_id INTEGER
);

CREATE TABLE IF NOT EXISTS dim_assigned_calls(
id INTEGER,
call_id INTEGER,
caller_id VARCHAR,
assigned_calls_to_agent_id INTEGER,
complaint_status VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_resolved_calls(
id INTEGER,
resolved_call_id INTEGER,
caller_id VARCHAR,
agent_id INTEGER,
assigned_to_id INTEGER,
complaint_status VARCHAR,
resolution_duration_in_hours INTEGER
);
CREATE TABLE IF NOT EXISTS dim_complaints(
id INTEGER,
call_id INTEGER,
complaint_topic VARCHAR,
complaint_status VARCHAR);

CREATE TABLE IF NOT EXISTS dim_agents(
id INTEGER,
agent_id INTEGER,
agents_grade_level VARCHAR);

CREATE TABLE IF NOT EXISTS fact_call_resolution(
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
call_duration_in_secs INTEGER);

INSERT INTO dim_all_calls(
id,
call_id,
caller_id,
agent_id,
call_type,
assigned_to_id,
call_duration_in_secs,
call_ended_by_agent
);

SELECT
a.id,
a.call_id,
a.caller_id,
a.recieving_agent_id as agent_id,
a.call_type,
a.assigned_to_id,
a.call_duration_in_secs,
a.call_ended_by_agent
FROM weserve.all_calls a;

INSERT INTO dim_recieved_calls(
id,
recieved_call_id,
inbound_caller_id,
call_duration_in_secs,
recieving_agent_id,
assigned_to_id
);

SELECT
r.id,
r.call_id as recieved_call_id,
r.caller_id as inbound_caller_id,
r.call_duration_in_secs,
r.recieving_agent_id,
r.assigned_to_id
FROM weserve.recieved_calls r;

INSERT INTO dim_resolved_calls(
id,
resolved_call_id,
caller_id,
recieving_agent_id,
assigned_to_id,
complaint_status,
resolution_duration_in_hours
);

SELECT
re.id,
re.call_id as resolved_call_id,
re.caller_id,
re.recieving_agent_id,
re.assigned_to_id,
re.complaint_status,
re.resolution_duration_in_hours
FROM weserve.resolved_calls re;

INSERT INTO dim_assigned_calls(
id,
call_id,
caller_id,
assigned_calls_to_agent_id,
complaint_status
);
SELECT
a.id,
a.call_id,
caller_id,
a.assigned_calls_to_agent_id,
a.complaint_status
FROM weserve.assigned_calls a;

INSERT INTO dim_complaints(
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
FROM weserve.complaints c;


INSERT INTO dim_agents(
id,
agent_id,
agents_grade_level
)
SELECT
da.id,
da.recieving_agent_id as agent_id,
da.agents_grade_level
FROM weserve.agents da;

INSERT INTO dim_call_resolution(
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
call_duration_in_secs
);
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
a.call_duration_in_secs
FROM weserve.all_calls a
LEFT JOIN weserve.assigned_calls asi
ON
a.call_id = asi.call_id
LEFT JOIN weserve.resolved_calls res
ON
a.call_id = res.call_id
LEFT JOIN weserve.complaints c
ON
a.call_id = c.call_id
LEFT JOIN weserve.recieved_calls rec
ON 
a.call_id = rec.call_id
LEFT JOIN weserve.agents ag
ON
a.call_id = ag.call_id
    """)

with open('transform.csv', 'w') as f:
    file= csv.writer(f, delimiter=',' )
    file.writerow(transform_tables)
    file.writerows(cursor.fetchall())

conn.close()