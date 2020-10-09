# Errors

In case of an error, the response payload is an RFC7807 Problem Details object:

Name   | Type   | Default    | Description
------ | ------ | ---------- | ---------------------------------
type   | string | _required_ | Error type.
title  | string | _required_ | Human-readable short description.
detail | string | _optional_ | Detailed error description.
status | int    | _required_ | HTTP-compatible status code. The same code is in response properties.

## Troubleshooting by status code

- **400 Bad Request** – Failed to parse JSON payload of the message or endpoint-specific validation failed.
- **403 Forbidden** – Authorization failed. Check out Authorization section of the endpoint.
- **404 Not Found** – The entity doesn't exist in the DB or expired.
- **405 Method Not Allowed** – Unknown `method` property value in the request.
- **422 Unprocessable Entity** – DB query error or some logic error.
- **424 Failed Dependency** – The backend responded with an error.
- **500 Internal Server Error** – A low-level problem occurred on the server.
- **503 Service Unavailable** – The service is unable to complete the request due to lack of backend capacity.

## Error types

One must rely on the `type` field of the error for error identification, not the `title` nor `status`.
The following types are a part of the service's API and are guaranteed to maintain compatibility.

- `access_denied` – The action was forbidden by [authorization](authz.md#Authorization).
- `agent_not_entered_the_room` – The agent must preliminary make [room.enter](room/enter.md#room.enter) request.
- `authorization_failed` – Authorization request failed due to a network error or another reason.
- `backend_recording_missing` – The backend responded that it doesn't have the recording for the RTC.
- `backend_request_failed` – The backend responded with an error code.
- `backend_request_timed_out` – The backend request didn't finished in a reasonable time.
- `backend_not_found` – The backend that hosted the RTC went offline.
- `capacity_exceeded` – There's no free capacity left on the backend to connect to.
- `database_connection_acquisition_failed` – The service couldn't obtain a DB connection from the pool.
- `database_query_failed` – The database returned an error while executing a query.
- `invalid_jsep_format` – Failed to determine whether the SDP is recvonly.
- `invalid_sdp_type` – Failed to parse SDP type or an SDP answer is received.
- `invalid_subscription_object` – An object for dynamic subscription is not of format `["rooms", UUID, "events"]`.
- `message_building_failed` – An error occurred while building a message to another service.
- `message_handling_failed` – An incoming message is likely to have non-valid JSON payload or missing required properties.
- `message_parsing_failed` – Failed to parse a message from another service.
- `no_available_backends` – No backends found to host the RTC.
- `not_implemented` – The requested feature is not supported.
- `publish_failed` – Failed to publish an MQTT message.
- `resubscription_failed` – The services has failed to resubscribe to topics after reconnect.
- `room_closed` - The [room](room.md#Room) exists but already closed.
- `room_not_found` – The [room](room.md#Room) is missing.
- `rtc_not_found` – An [RTC](rtc.md#Real-time_Connection) is missing or closed.
- `stats_collection_failed` – Couldn't collect metrics from one of the sources.
- `unknown_method` – An unsupported value in `method` property of the request message.
