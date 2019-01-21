# Create

Creates a real-time connection to available backend through signal messages and then media may be sent.

**Request**

```bash
pub agents/${ME}/api/v1/out/${APP_NAME}
```

**Topic parameters**

Name     | Type   | Default    | Description
-------- | ------ | ---------- | ------------------
ME       | string | _required_ | Agent identifier
APP_NAME | string | _required_ | Name of the application

**Properties**

Name             | Type   | Default    | Description
---------------- | ------ | ---------- | ------------------
type             | string | _required_ | Always `request`
method           | string | _required_ | Always `rtc.create`
response_topic   | string | _required_ | Always `agents/${ME}/api/v1/in/${APP_NAME}`
correlation_data | string | _required_ | The same value will be in a response

**Payload**

Name              | Type   | Default    | Description
----------------- | ------ | ---------- | ------------------
room_id           | string | _required_ | Room where the real-time connection will be created

**Response**

Name              | Type   | Default    | Description
----------------- | ------ | ---------- | ------------------
jsep              | object | _optional_ | Returned only on offer requests and contains answer.

**Response**

If successful, the response payload contains an instance of **Real-Time Connection**.
