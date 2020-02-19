# Create

Create a real-time connection.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `rtc.create`.

**Payload**

Name              | Type   | Default    | Description
----------------- | ------ | ---------- | ------------------
room_id           | String | _required_ | A room where the real-time connection will be created.



## Unicast response

If successful, the response payload contains a **Real-Time Connection** object.
