# Update

Update a Room which holds Real-Time Connections.

## Request

PATCH /api/v1/rooms/{id}

**Properties**

Name         | Type       | Default    | Description
------------ | ---------- | ---------- | ------------------
id           | String     | _required_ | The room identifier. The room must not be expired.

**Payload**

Name         | Type       | Default    | Description
------------ | ---------- | ---------- | ------------------
time         | [i64, i64) | _optional_ | A [lt, rt) range of unix time (seconds) or null (unbounded).
reserve      | i32        | _optional_ | The number of slots for subscribers to reserve on the server.
tags         | json       | {}         | Arbitrary tags object associated with the room.
classroom_id | uuid       | _optional_ | Related classroom id.


## Response

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
