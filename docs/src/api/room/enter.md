# Enter

Subscribe to the room's events. Creates RTC for the agent upon entering
a minigroup.


## Request

POST /api/v1/rooms/{id}/enter

**Properties**

Name     | Type       | Default    | Description
-------- | ---------- | ---------- | ------------------
id       | Uuid       | _required_ | The room identifier. The room must be opened.

**Payload**

Name        | Type       | Default    | Description
----------- | ---------- | ---------- | ------------------
agent_label | String     | _required_ | Agent label which is used for MQTT Gateway.


## Response

If successful, the response contain status only.
