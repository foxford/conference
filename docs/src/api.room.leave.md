# Leave

Unsubscribe from the room's events.

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
method           | string | _required_ | Always `room.leave`.
response_topic   | string | _required_ | Always `agents/${ME}/api/v1/in/${APP_NAME}`.
correlation_data | string | _required_ | The same value will be in a response.

**Payload**

Name     | Type       | Default    | Description
-------- | ---------- | ---------- | ------------------
id       | uuid       | _required_ | The room identifier.

**Response**

If successful, the response contain status only.
