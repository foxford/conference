# Read

Retrieve **Agent Writer Snapshots** list for a given room.

## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ----------------------------------
method           | String | _required_ | Always `writer_config_snapshot.read`.

**Payload**

Name    | Type | Default    | Description
------- | ---- | ---------- | --------------------
room_id | Uuid | _required_ | The room identifier.



## Unicast response

If successful, the response payload contains list of all snapshots.
