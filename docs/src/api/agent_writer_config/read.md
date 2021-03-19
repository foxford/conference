# Read

Retrieve **Agent Writer Config** state.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ----------------------------------
method           | String | _required_ | Always `agent_writer_config.read`.

**Payload**

Name    | Type | Default    | Description
------- | ---- | ---------- | --------------------
room_id | Uuid | _required_ | The room identifier.



## Unicast response

If successful, the response payload contains a requested **Agent Writer Config** state.
