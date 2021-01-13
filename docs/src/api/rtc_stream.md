# RTC Stream

## Properties

Name       | Type       | Default    | Description
-----------| ---------- | ---------- | ---------------------------------------------------------------
id         |       uuid | _required_ | The RTC stream identifier.
rtc_id     |       uuid | _required_ | The [RTC](rtc.md#properties) to which stream stream belongs to.
label      |     string | _required_ | Arbitrary label associated with the stream.
sent_by    |   agent_id | _required_ | Agent ID of the writer.
time       | [int, int] | _optional_ | Absolute start and stop timestamps of the stream. If missing then the stream is not yet started.
created_at |        int | _required_ | Absolute timestamp of the stream creation.


## Lifecycle events

### rtc_stream.update

Emited when:

* got `webrtcup` event from janus backend which means that the writer has started streaming.
* got `hangup` event from janus backend which means that the writer stopped streaming.
* got `detach` event from janus backend which means that the writer's handle got detached
(e.g. in acse of network problems).
* janus backend went offline.

**URI:** `rooms/:room_id/events` where `room_id` is the RTC's room_id.

**Label:** `rtc_stream.update`.

**Payload:** [RTC Stream](#properties) object.
