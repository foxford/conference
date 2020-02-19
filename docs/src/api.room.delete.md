# Delete

Delete a Room which holds Real-Time Connections.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `room.delete`.

**Payload**

Name   | Type | Default    | Description
------ | ---- | ---------- | ------------------
id     | Uuid | _required_ | The room identifier. The room must not be expired.



## Unicast response

If successful, the response payload contains a deleted **Room** object.
