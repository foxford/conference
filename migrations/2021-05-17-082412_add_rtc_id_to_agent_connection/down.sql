ALTER TABLE agent_connection DROP CONSTRAINT agent_connection_pkey;
ALTER TABLE agent_connection ADD PRIMARY KEY (agent_id);
ALTER TABLE agent_connection DROP COLUMN rtc_id;
