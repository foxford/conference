# Read

Retrieve own **Agent Reader Config** state.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ----------------------------------
method           | String | _required_ | Always `agent_reader_config.read`.

**Payload**

Name    | Type | Default    | Description
------- | ---- | ---------- | --------------------
room_id | Uuid | _required_ | The room identifier.



## Unicast response

If successful, the response payload contains a requested **Agent Reader Config** state.
