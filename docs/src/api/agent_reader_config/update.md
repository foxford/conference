# Update

Update or initialize own **Agent Reader Configs** in bulk.
Configs get merged into the current state so one may send only diffs.

One must enter the room first and the room must be opened.

The room must have `owned` RTC sharing policy.

The writer for which we want to apply config for must have created an owned RTC in the room.

## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ----------------------------------------
method           | String | _required_ | Always `agent_reader_connection.update`.

**Payload**

Name    | Type     | Default    | Description
------- | -------- | ---------- | ----------------------------------------------
room_id |     uuid | _required_ | The **Room** identifier.
configs | [object] | []         | Array of **[Agent Reader Config Item](../agent_reader_config.html#agent-reader-config-item)** objects.

## Unicast response

If successful, the response payload contains current
**[Agent Reader Config](../agent_reader_config.html#agent-reader-config)** state for all RTCs
in the room for the agent that made the request.
