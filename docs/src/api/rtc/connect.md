# Connect

Connect to the real-time connection to send signal messages and media.
The method isn't available for `none` backend.

Creates a Janus handle for the particular agent.

If there's already a stream present for the RTC then the returned handle would be bound to the
Janus instance that hosts this stream.

If there's no stream yet then the handle is being balanced to the instance with the least number
of active RTC streams.

The agent must perform signaling using [signal.create](./signal/create.md) method first.
Note that in order to connect with intent = `read` the SDP offer passed to `signal.create` must be
of type `recvonly` or `sendrecv`. And vice versa for intent = `write`: the SDP must be of type
`sendonly` or `sendrecv`. If you have initially passed an SDP of non-compatible type with the intent
you want to connect to the rtc you need to renegotiate the SDP using
[signal.update](../signal/update.md) method.


## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
method           | String | _required_ | Always `rtc.connect`.

**Payload**

Name      | Type   | Default    | Description
--------- | ------ | ---------- | ------------------------------------
id        | String | _required_ | A real-time connection identifier.
intent    | String | read       | `write` or `read`.
label     | String | _optional_ | An arbitrary label to mark the stream. Required only with intent = `write`.


## Unicast response

If successful, the response payload contains an empty JSON object.
