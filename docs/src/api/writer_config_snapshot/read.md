# Read

Retrieve **Agent Writer Snapshots** list for a given room.

## Request

GET /api/v1/rooms/{room_id}/configs/writer/snapshot

**Properties**

Name    | Type | Default    | Description
------- | ---- | ---------- | --------------------
room_id | Uuid | _required_ | The room identifier.



## Response

If successful, the response payload contains list of all snapshots.
