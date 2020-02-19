# Callback

Send back a response on a message.



## Multicast request

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
type             | String | _required_ | Always `response`.
correlation_data | String | _required_ | The exact same value from a related request.
status           | i32    | _required_ | A status code.

**Payload**

JSON object.
