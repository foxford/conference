# List

List active agents.

## Request

GET /api/v1/rooms/{room_id}/agents?{offset}&{limit}

**Properties**

Name       | Type       | Default    | Description
---------- | ---------- | ---------- | ------------------
room_id    | string     | _required_ | Returns only objects that belong to the room. The room must be opened.
offset     | int        | _optional_ | Returns objects starting from the specified index.
limit      | int        |         25 | Limits the number of objects in the response.

## Response

If successful, the response payload contains the list of **Agent** objects.
