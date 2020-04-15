# List

List active agents.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | string | _required_ | Always `agent.list`.

**Payload**

Name       | Type       | Default    | Description
---------- | ---------- | ---------- | ------------------
room_id    | string     | _required_ | Returns only objects that belong to the room. The room must be opened.
offset     | int        | _optional_ | Returns objects starting from the specified index.
limit      | int        |         25 | Limits the number of objects in the response.



## Unicast response

If successful, the response payload contains the list of **Agent** objects.
