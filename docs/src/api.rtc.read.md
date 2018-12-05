# Read

Read a real-time connection in order to initialize signaling phase and receive media.

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
method           | string | _required_ | always `rtc.read`
correlation_data | string | _required_ | The same value will be in a response
response_topic   | string | _required_ | It should match `agents/${ME}/api/v1/out/${APP_NAME}`

**Payload**

Name       | Type   | Default    | Description
---------- | ------ | ---------- | ------------------
room_id    | string | _required_ | Room where the real-time connection will be created
lable      | string | _required_ | An answer and ice candidates will contain this label
jsep       | string | _required_ | An offer (must satisfy all constraints set by the rpc creator)