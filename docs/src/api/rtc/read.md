# Read

Read the real-time connection.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `rtc.read`.

**Payload**

Name       | Type   | Default    | Description
---------- | ------ | ---------- | ------------------
id         | String | _required_ | The Real-time connection identifier.



## Unicast response

If successful, the response payload contains the **Real-Time Connection** instance.
