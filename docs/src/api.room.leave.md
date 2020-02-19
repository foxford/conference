# Leave

Unsubscribe from the room's events.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `room.leave`.

**Payload**

Name     | Type       | Default    | Description
-------- | ---------- | ---------- | ------------------
id       | Uuid       | _required_ | The room identifier. The room must not be expired.



## Unicast response

If successful, the response contain status only.
