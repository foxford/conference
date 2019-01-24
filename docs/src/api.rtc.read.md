# Read

Picks up the real-time connection through signal messages and then media may be received.

*NOTE: If Janus Gateway is used as a backend, a handle to the conference plugin for the patricular agent will be created. A session will be created when necessary.*

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
method           | string | _required_ | Always `signal.read`
response_topic   | string | _required_ | Always `agents/${ME}/api/v1/in/${APP_NAME}`
correlation_data | string | _required_ | The same value will be in a response

**Payload**

Name       | Type   | Default    | Description
---------- | ------ | ---------- | ------------------
id         | string | _required_ | Real-time connection identifier

**Response**

If successful, the response payload contains an instance of **Real-Time Connection**.
