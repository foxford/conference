# Agent Writer Config

**Agent Writer Config** affects the writer and hence all of his readers.
Muting the writer on this level also prevents media to be recorded.
One can also set REMBs to control requested media bitrate.

## Properties

Name    | Type     | Default    | Description
------- | -------- | ---------- | ------------------------------------------
room_id |     uuid | _required_ | The **Room** identifier.
configs | [object] | []         | The list of **Agent Writer Config Items**.

# Agent Writer Config Item

## Properties

Name       | Type     | Default    | Description
---------  | -------- | ---------- | -----------------------------------------------
agent_id   | agent_id | _required_ | Writer identifier which the config applies to.
send_video |     bool | true       | Whether the writer is allowed to publish video.
send_audio |     bool | true       | Whether the writer is allowed to publish audio.
video_remb |      int | _required_ | Maximum video bitrate requested for the writer.
audio_remb |      int | _required_ | Maximum audio bitrate requested for the writer.
