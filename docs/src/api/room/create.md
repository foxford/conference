# Create

Create a Room which holds Real-Time Connections.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `room.create`.


**Payload**

Name              | Type       | Default    | Description
----------------- | ---------- | ---------- | ------------------
time              | [i64, i64) | _required_ | A [lt, rt) range of unix time (seconds) or null (unbounded).
audience          | String     | _required_ | The room audience.
backend           | String     | none       | The room backend. Available values: janus, none.
subscribers_limit | i32        | _optional_ | The maximum number of simultaneously entered subscribers allowed.


## Unicast response

If successful, the response payload contains a **Room** object.

## Broadcast event

A notification is being sent to the _audience_ topic.

**URI:** `audiences/:audience/events`

**Label:** `room.create`.

**Payload:** created [room](../room.md#room) object.
