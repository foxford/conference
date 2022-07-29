# List

List of real-time connections.



## Request

GET /api/v1/rooms/{id}/rtcs?{offset}&{limit}

**Properties**

Name       | Type   | Default    | Description
---------- | ------ | ---------- | ------------------
room_id    | String | _required_ | Returns only objects that belong to the room. The room must be opened.
offset     | i32    | _optional_ | Returns only objects starting from the specified index.
limit      | i32    |         25 | Limits the number of objects in the response.



## Response

If successful, the response payload contains the list of **Real-Time Connection** objects.
