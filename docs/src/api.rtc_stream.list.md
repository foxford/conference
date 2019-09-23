# List

List streams of real-time connections.
The method isn't available for `none` backend.

**Request**

```bash
pub agents/${ME}/api/v1/out/${APP_NAME}
```

**Topic parameters**

Name     | Type   | Default    | Description
-------- | ------ | ---------- | ------------------
ME       | string | _required_ | Agent identifier.
APP_NAME | string | _required_ | Name of the application.

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
type             | string | _required_ | Always `request`.
method           | string | _required_ | Always `rtc_stream.list`.
response_topic   | string | _required_ | Always `agents/${ME}/api/v1/in/${APP_NAME}`.
correlation_data | string | _required_ | The same value will be in a response.

**Payload**

Name       | Type       | Default    | Description
---------- | ---------- | ---------- | ------------------
room_id    | string     | _required_ | Returns only objects that belong to the room. The room must be opened.
rtc_id     | string     | _optional_ | Returns only objects that belong to the rtc.
time       | [int, int] | _optional_ | Returns only objects that time overlaps with [lt, rt) range of unix time (seconds) or null (unbounded).
offset     | int        | _optional_ | Returns objects starting from the specified index.
limit      | int        |         25 | Limits the number of objects in the response.

**Response**

If successful, the response payload contains the list of **Real-Time Connection Stream** objects.
