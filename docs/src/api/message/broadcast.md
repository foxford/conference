# Broadcast

Send a message to all agents in the room.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `message.broadcast`.

**Payload**

Name              | Type       | Default    | Description
----------------- | ---------- | ---------- | ------------------
room_id           | Uuid       | _required_ | A destination room identifier. The room must be opened.
data              | JsonObject | _required_ | JSON object.
label             | String     | _optional_ | A label to group messages by in metrics.



## Unicast response

If successful, the response payload contains a JSON object.
