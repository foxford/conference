# Update

Update or initialize own **Agent Writer Configs** in bulk.
Configs get merged into the current state so one may send only diffs.

One must enter the room first and the room must be opened.

## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ----------------------------------------
method           | String | _required_ | Always `agent_writer_connection.update`.

**Payload**

Name    | Type     | Default    | Description
------- | -------- | ---------- | ----------------------------------------------
room_id |     uuid | _required_ | The **Room** identifier.
configs | [object] | []         | Array of **[Agent Writer Config Item](../agent_writer_config.md#agent-writer-config-item)** objects.

## Unicast response

If successful, the response payload contains current 
**[Agent Writer Config](../agent_writer_config.md#agent-writer-config)** state for all RTCs in the room.

## Broadcast event

A notification is being sent to the _audience_ topic.

**URI:** `audiences/:audience/events`

**Label:** `agent_writer_config.update`.

**Payload:** current **Agent Writer Config** state for all RTCs in the room.
