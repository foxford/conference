# Authorization

In order to authorize an **action** performed by a **subject** to an **object**, the application sends a `POST` request to the authorization endpoint.

**Example**

```json
{
    "subject": {
        "namespace": "iam.example.org",
        "value": ["accounts", "123e4567-e89b-12d3-a456-426655440000"]
    },
    "object": {
        "namespace": "conference.example.org",
        "value": ["rooms", "123e4567-e89b-12d3-a456-426655440000", "rtcs", "321e7654-e89b-12d3-a456-426655440000"]
    },
    "action": "read"
}
```

Subject's namespace and account identifier are retrieved from `audience` and `account_label` properties of MQTT message respectively. URI of authorization endpoint, object and anonymous namespaces are configured through the application configuration file.

Possible values for `SUBJECT`:

subject                   |
------------------------- | -
["accounts", ACCOUNT_ID]  |

Possible values for `OBJECT` and `ACTION`:

object / action                        | create | read | update | delete | list | join |
-------------------------------------- | ------ | ---- | ------ | ------ | ---- | ---- |
["rooms"]                              |      + |      |        |        |    + |      |
["rooms", ROOM_ID]                     |        |    + |      + |      + |      |    + |
["rooms", ROOM_ID, "agents"]           |        |      |        |        |    + |      |
["rooms", ROOM_ID, "agents", AGENT_ID] |        |    + |      + |        |      |      |
["rooms", ROOM_ID, "rtcs"]             |      + |      |        |        |    + |      |
["rooms", ROOM_ID, "rtcs", RTC_ID]     |        |    + |      + |      + |      |      |