# Agent

## Properties

Name       | Type     | Default    | Description
-----------| -------- | ---------- | ----------------------------------------------------
id         |     uuid | _required_ | The identifier of the agent's entrance to the room.
agent_id   | agent_id | _required_ | The agent's ID.
room_id    |     uuid | _required_ | The room ID.
created_at |      int | _required_ | Entrance timestamp in seconds.
status     |   string | _required_ | See below.

## Statuses

* `in_progress` – the agent has called [room.enter](room/enter.md) request but the broker has not yet confirmed the dynamic subscription to the room's events.
* `ready` – the agent has entered the room.
