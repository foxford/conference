# Create

Create a real-time connection to send signal messages and then media.

*NOTE: If Janus Gateway is used as a backend, a handle to the conference plugin for the patricular agent and a session will be created when necessary.*

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

If successful, the response payload contains a **Real-Time Connection** object.
