# List

List streams of real-time connections.
The method isn't available for `none` backend.



## Request

GET /api/v1/rooms/{room_id}/streams?{rtc_id}&{time}&{offset}&{limit}

**Properties**

Name       | Type       | Default    | Description
---------- | ---------- | ---------- | ------------------
room_id    | String     | _required_ | Returns only objects that belong to the room. The room must be opened.
rtc_id     | String     | _optional_ | Returns only objects that belong to the rtc.
time       | [i64, i64) | _optional_ | Returns only objects that time overlaps with [lt, rt) range of unix time (seconds) or null (unbounded).
offset     | i32        | _optional_ | Returns objects starting from the specified index.
limit      | i32        |         25 | Limits the number of objects in the response.



## Response

If successful, the response payload contains the list of **Real-Time Connection Stream** objects.
