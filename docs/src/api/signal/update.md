# Update

Renegotiate SDP.

The agent must perform initial signaling using [signal.create](./create.md) method
to obtain `handle_id`.


## Multicast request

**Properties**

Name   | Type   | Default    | Description
------ | ------ | ---------- | -----------------------
method | String | _required_ | Always `signal.update`.

**Payload**

Name      | Type       | Default    | Description
--------- | ---------- | ---------- | -------------------------------------------------
handle_id | String     | _required_ | WebRTC connection handle identifier.
jsep      | JsonObject | _required_ | **Ice candidate** generated by RTCPeerConnection.


## Unicast response

Name      | Type       | Description
--------- | ---------- | ----------------------------------------------
jsep      | JsonObject | SDP **answer** to pass into RTCPeerConnection.