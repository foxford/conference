# Update

Create/update groups with agents and RTC reader configs for agents in different groups.

One must enter the room first and the room must be opened.

The room must have `owned` RTC sharing policy.

## Request

POST /api/v1/rooms/{room_id}/groups

**Properties**

| Name    | Type | Default    | Description              |
|---------|------|------------|--------------------------|
| room_id | uuid | _required_ | The **Room** identifier. |

**Payload**

```json
[
  {
    "number": 0,
    "agents": ["web.Z2lkOi8vc3RvZWdlL1VzZXI6OkFnZW50LzcxNjM3NDE=.usr.foxford.ru"]
  },
  {
    "number": 1,
    "agents": ["web.Z2lkOi8vc3RvZWdlL1VzZXI6OkFnZW50LzYxMzI1MzE=.usr.foxford.ru"]
  }
]
```

| Name   | Type       | Description                |
|--------|------------|----------------------------|
| number | int        | Group number               |
| agents | [agent_id] | Array of agent identifiers | 


## Response

If successful, the response status code is 200

## Broadcast event

A notification is being sent to the _room_ topic

**URI:** `rooms/:room_id/events`

**Label:** `group.update`

**Payload:** empty
