# Read

Retrieve a Room which holds Real-Time Connections.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `room.read`.

**Payload**

Name   | Type | Default    | Description
------ | ---- | ---------- | ------------------
id     | Uuid | _required_ | The room identifier.



## Unicast response

If successful, the response payload contains a requested [room](../room.md#properties) object.
