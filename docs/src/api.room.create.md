# Create

Create a Room which holds Real-Time Connections.

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
method           | string | _required_ | Always `room.create`
response_topic   | string | _required_ | Always `agents/${ME}/api/v1/in/${APP_NAME}`
correlation_data | string | _required_ | The same value will be in a response

**Payload**

Name              | Type                       | Default    | Description
----------------- | -------------------------- | ---------- | ------------------
start             | RFC3339-formatted DateTime | _required_ | A time when Room becomes 'open' for certain operations.
end               | RFC3339-formatted DateTime | _required_ | A time when Room becomes 'closed' for certain operations.
audience          | string                     | _required_ | Room's audience.

**Response**

If successful, the response payload contains a **Room** object.
