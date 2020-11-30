# Room

## Properties

Name       | Type       | Default    | Description
-----------| ---------- | ---------- | ----------------------------------------------------
id         |       uuid | _required_ | The room identifier.
audience   |     string | _required_ | The audience of the room.
time       | [int, int] | _required_ | Opening and closing timestamps in seconds.
created_at |        int | _required_ | Room creation timestamp in seconds.
backend    |     string | _required_ | Room backend, either `janus` or `none`.
reserve    |        int | _optional_ | The number of slots for agents reserved on the backend.
tags       |       json | {}         | Arbitrary tags object associated with the room.


Room can be unbounded, ie its closing timestamp is null.
To avoid rooms being stuck in unvacuumed state, room closure time will be set as 6 hours from first rtc creation.
This 6 hours duration serves as timeout for vacuums.

When the room closure time becomes bounded (either by creating rtc or it was bounded from the start),
closure=unbounded update is prohibited to avoid erasing this 6 hours timeout.

## Lifecycle events

### room.close event

If either
  * the room was updated so that the closure datetime was moved from future into the past,
  * the room was vacuumed

`room.close` event will be sent to room topic and tenant topics.
This event is not guaranteed to be unique for a room, that is two `room.close` events could be sent by the service.

**URI:** `rooms/:room_id/events`
**URI:** `audiences/:audience/events`

**Label:** `room.close`.

**Payload:** [room](#properties) object.
