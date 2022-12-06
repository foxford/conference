# List

List group agents.

## Request

GET /api/v1/rooms/{room_id}/groups?{within_group}

**Properties**

| Name         | Type   | Default    | Description                                                                     |
|--------------|--------|------------|---------------------------------------------------------------------------------|
| room_id      | string | _required_ | Returns only objects that belong to the room. The room must be opened.          |
| within_group | bool   | _optional_ | Returns agents of the same group in which there is an agent who sent a request. |

## Response

If successful, the response payload contains the list of groups agents:

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
