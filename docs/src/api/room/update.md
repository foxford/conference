# Update

Update a Room which holds Real-Time Connections.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `room.update`.

**Payload**

Name     | Type       | Default    | Description
-------- | ---------- | ---------- | ------------------
id       | String     | _required_ | The room identifier. The room must not be expired.
time     | [i64, i64) | _optional_ | A [lt, rt) range of unix time (seconds) or null (unbounded).
audience | String     | _optional_ | The room audience.
backend  | String     | _optional_ | The room backend. Available values: janus, none.
reserve  | i32        | _optional_ | The number of slots for subscribers to reserve on the server.
tags     | json       | {}         | Arbitrary tags object associated with the room.


## Unicast response

If successful, the response payload contains an updated **Room** object.

## Broadcast event

A notification is being sent to the _audience_ topic.

**URI:** `audiences/:audience/events`

**Label:** `room.update`.

**Payload:** [room](../room.md#properties) object.

If the room closure date had been in the future but was moved by the update into the past, a notification will be sent to the _room_ topic.
Clients should not rely on this notification being unique.
That is this notification can reoccur even if it was sent before.

**URI:** `rooms/:room_id/events`

**Label:** `room.close`.

**Payload:** [room](../room.md#properties) object.
