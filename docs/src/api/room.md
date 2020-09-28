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
