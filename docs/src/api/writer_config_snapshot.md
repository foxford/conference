# Writer Config Snapshot

**Agent Writer Config** is cumulative across multiple updates. **Writer Config Snapshot** stores each update so history of audio/video mutes is available for transcoding.

## Properties

## Properties

Name       | Type     | Default    | Description
---------  | -------- | ---------- | -----------------------------------------------
id         | uuid     | _required_ |
rtc_id     | uuid     | _required_ | Rtc this config belongs to.
send_video |     bool | true       | Whether the writer is allowed to publish video.
send_audio |     bool | true       | Whether the writer is allowed to publish audio.
video_remb |      int | _required_ | Maximum video bitrate requested for the writer.
