TRUNCATE agent_connection;
ALTER TABLE agent_connection ADD COLUMN rtc_id UUID NOT NULL;

ALTER TABLE agent_connection
ADD CONSTRAINT agent_conenction_rtc_id_fk
FOREIGN KEY (rtc_id) REFERENCES rtc (id) ON DELETE CASCADE;

ALTER TABLE agent_connection DROP CONSTRAINT agent_connection_pkey;
ALTER TABLE agent_connection ADD PRIMARY KEY (agent_id, rtc_id);
