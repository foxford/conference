# Create

Create a Room which holds Real-Time Connections.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `room.create`.


**Payload**

Name     | Type       | Default    | Description
-------- | ---------- | ---------- | ------------------
time     | [i64, i64) | _required_ | A [lt, rt) range of unix time (seconds) or null (unbounded).
audience | String     | _required_ | The room audience.
backend  | String     | none       | The room backend. Available values: janus, none.



## Unicast response

If successful, the response payload contains a **Room** object.
