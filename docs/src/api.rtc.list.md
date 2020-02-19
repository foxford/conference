# List

List of real-time connections.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `rtc.list`.

**Payload**

Name       | Type   | Default    | Description
---------- | ------ | ---------- | ------------------
room_id    | String | _required_ | Returns only objects that belong to the room. The room must be opened.
offset     | i32    | _optional_ | Returns only objects starting from the specified index.
limit      | i32    |         25 | Limits the number of objects in the response.



## Unicast response

If successful, the response payload contains the list of **Real-Time Connection** objects.
