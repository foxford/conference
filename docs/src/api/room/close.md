# Close

Close a Room which holds Real-Time Connections.

## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `room.close`.

**Payload**

Name         | Type       | Default    | Description
------------ | ---------- | ---------- | ------------------
id           | String     | _required_ | The room identifier. The room must not be expired.


## Unicast response

If successful, the response payload contains an updated **Room** object.

## Broadcast event

A notification is being sent to the _audience_ topic.

**URI:** `audiences/:audience/events`

**Label:** `room.update`.

**Payload:** [room](../room.md#properties) object.

**URI:** `rooms/:room_id/events`

**Label:** `room.close`.

**Payload:** [room](../room.md#properties) object.

**URI:** `audiences/:audience/events`

**Label:** `room.close`.

**Payload:** [room](../room.md#properties) object.
