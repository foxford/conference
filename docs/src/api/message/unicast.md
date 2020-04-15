# Unicast

Send a message to specific agent in the room.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `message.unicast`.

**Payload**

Name              | Type       | Default    | Description
----------------- | ---------- | ---------- | ------------------
room_id           | Uuid       | _required_ | A destination room identifier. The room must be opened.
agent_id          | String     | _required_ | A destination agent identifier.
data              | JsonObject | _required_ | JSON object.



## Unicast response

If successful, the response payload contains a JSON object.
