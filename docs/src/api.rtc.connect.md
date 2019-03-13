# Connect

Connect to the real-time connection to send signal messages and media.

*NOTE: If Janus Gateway is used as a backend, a handle to the conference plugin for the patricular agent will be created.*

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
method           | string | _required_ | Always `rtc.connect`.
response_topic   | string | _required_ | Always `agents/${ME}/api/v1/in/${APP_NAME}`.
correlation_data | string | _required_ | The same value will be in a response.

**Payload**

Name       | Type   | Default    | Description
---------- | ------ | ---------- | ------------------
id         | string | _required_ | A real-time connection identifier.

**Response**

If successful, the response payload contains a **Real-Time Connection Handle Identifier**.
