# Real-Time Connection

## Properties

Name       | Type | Default    | Description
-----------| ---- | ---------- | --------------------------------------------------
id         | uuid | _required_ | The RTC identifier.
room_id    | uuid | _required_ | The room's identifier to which the RTC belongs to.
created_at |  int | _required_ | RTC creation timestamp in seconds.


## Lifecycle events.

### rtc.create event

Emited when an RTC gets created.

**URI:** `rooms/:room_id/events`

**Label:** `rtc.create`.

**Payload:** created [Real-Time Connection](#properties) object.
