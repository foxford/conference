# Callback

Send back a response on a message.

**Response**

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
type             | string | _required_ | Always `response`.
correlation_data | string | _required_ | The exact same value from a related request.
status           | int    | _required_ | A status code.

**Payload**

JSON object.
