# Connect

Connect to the real-time connection to send signal messages and media.
The method isn't available for `none` backend.

Creates a Janus handle for the particular agent.

If there's already a stream present for the RTC then the returned handle would be bound to the
Janus instance that hosts this stream.

If there's no stream yet then the handle is being balanced to the instance with the least number
of active RTC streams.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `rtc.connect`.

**Payload**

Name | Type   | Default    | Description
---- | ------ | ---------- | ------------------
id   | String | _required_ | A real-time connection identifier.
role | String | subscriber | `publisher` or `subscriber`.



## Unicast response

If successful, the response payload contains a **Real-Time Connection Handle Identifier**.
