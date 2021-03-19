# Agent Reader Config

Each agent sets his own config for each writer so he can mute writers selectively just for himself.

## Properties

Name    | Type     | Default    | Description
------- | -------- | ---------- | ------------------------------------------
room_id |     uuid | _required_ | The **Room** identifier.
configs | [object] | []         | The list of **Agent Reader Config Items**.

# Agent Reader Config Item

## Properties

Name          | Type     | Default    | Description
------------- | -------- | ---------- | ----------------------------------------------
agent_id      | agent_id | _required_ | Writer identifier which the config applies to.
receive_video |     bool | true       | Whether to receive video from the writer.
receive_audio |     bool | true       | Whether to receive audio from the writer.
