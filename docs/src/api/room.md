# Room

## Properties

Name              | Type       | Default    | Description
----------------- | ---------- | ---------- | ----------------------------------------------------
id                |       uuid | _required_ | The room identifier.
audience          |     string | _required_ | The audience of the room.
time              | [int, int] | _required_ | Opening and closing timestamps in seconds.
created_at        |        int | _required_ | Room creation timestamp in seconds.
backend           |     string | _required_ | Room backend, either `janus` or `none`.
subscribers_limit |        int | _optional_ | The maximum number of simultaneously entered subscribers allowed.
