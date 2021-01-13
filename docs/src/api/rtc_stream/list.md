# List

List streams of real-time connections.
The method isn't available for `none` backend.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `rtc_stream.list`.

**Payload**

Name       | Type       | Default    | Description
---------- | ---------- | ---------- | ------------------
room_id    | String     | _required_ | Returns only objects that belong to the room. The room must be opened.
rtc_id     | String     | _optional_ | Returns only objects that belong to the rtc.
time       | [i64, i64) | _optional_ | Returns only objects that time overlaps with [lt, rt) range of unix time (seconds) or null (unbounded).
offset     | i32        | _optional_ | Returns objects starting from the specified index.
limit      | i32        |         25 | Limits the number of objects in the response.



## Unicast response

If successful, the response payload contains the list of [Real-Time Connection Stream](../rtc_stream.md#properties) objects.
