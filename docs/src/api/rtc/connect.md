# Connect

Connect to the real-time connection to send signal messages and media.
The method isn't available for `none` backend.

Creates a Janus handle for the particular agent.

If there's already a stream present for the RTC then the returned handle would be bound to the
Janus instance that hosts this stream.

If there's no stream yet then the handle is being balanced to the instance with the least number
of active RTC streams.



## Request

POST /api/v1/rtcs/{id}/streams

**Properties**

Name   | Type   | Default    | Description
------ | ------ | ---------- | ------------------
id     | String | _required_ | A real-time connection identifier.

**Payload**

Name   | Type   | Default    | Description
------ | ------ | ---------- | ------------------
intent | String | read       | `write` or `read`.



## Response

If successful, the response payload contains a **Real-Time Connection Handle Identifier**.
