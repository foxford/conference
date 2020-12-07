# Enter

Subscribe to the room's events.

One cannot enter the room when its closing time has come or when it doesn't have opening time.
If opening time is in the future then that's fine.

## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `room.enter`.

**Payload**

Name     | Type       | Default    | Description
-------- | ---------- | ---------- | ------------------
id       | Uuid       | _required_ | The room identifier.


## Unicast response

If successful, the response contain status only.

## Broadcast event

A notification is being sent to the _ROOM_ topic.

**URI:** `rooms/:room_id/events`

**Label:** `room.enter`.

**Payload:**
```json
{
  "id": ROOM_ID,
  "agent_id": ENTER_AGENT_ID
}
```
