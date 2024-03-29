# Create

Create a Room which holds Real-Time Connections.



## Request

POST /api/v1/rooms

**Payload**

Name               | Type       | Default    | Description
------------------ | ---------- | ---------- | ------------------
time               | [i64, i64) | _required_ | A [lt, rt) range of unix time (seconds) or null (unbounded).
audience           | String     | _required_ | The room audience.
backend            | String     | none       | [DEPRECATED] The room backend. Available values: janus, none.
rtc_sharing_policy | String     | none       | RTC sharing mode. Available values: none, shared, owned.
reserve            | i32        | _optional_ | The number of slots for subscribers to reserve on the server.
tags               | json       | {}         | Arbitrary tags object associated with the room.
classroom_id       | uuid       | _required_ | Related classroom id.

**Deprecation warning**

`backend` property is deprecated, use `rtc_sharing_policy` instead. For compatibility purpose
`backend = janus` implies `rtc_sharing_policy = shared` and `backend = none` implies
`rtc_sharing_policy = none`. If `rtc_sharing_policy` is specified then `backend` is being ignored.

## Response

If successful, the response payload contains a **Room** object.

## Broadcast event

A notification is being sent to the _audience_ topic.

**URI:** `audiences/:audience/events`

**Label:** `room.create`.

**Payload:** created [room](../room.md#room) object.
