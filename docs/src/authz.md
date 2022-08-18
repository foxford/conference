# Authorization

In order to authorize an **action** performed by a **subject** to an **object**, the application sends a `POST` request to the authorization endpoint.

**Example**

```json
{
    "subject": {
        "namespace": "iam.example.org",
        "value": "123e4567-e89b-12d3-a456-426655440000"
    },
    "object": {
        "namespace": "conference.example.org",
        "value": ["classrooms", "123e4567-e89b-12d3-a456-426655440000", "rtcs", "321e7654-e89b-12d3-a456-426655440000"]
    },
    "action": "read"
}
```

Subject's namespace and account label are retrieved from `audience` and `account_label` properties of MQTT message respectively. URI of authorization endpoint, object and anonymous namespaces are configured through the application configuration file.

Possible values for `OBJECT` and `ACTION`:

| object / action                              | create | read | update | list | subscribe |
|----------------------------------------------|--------|------|--------|------|-----------|
| ["classrooms"]                               | +      |      |        | +    |           |
| ["classrooms", CLASSROOM_ID]                 |        | +    | +      |      |           |
| ["classrooms", CLASSROOM_ID, "rtcs"]         | +      |      |        | +    |           |
| ["classrooms", CLASSROOM_ID, "rtcs", RTC_ID] |        | +    | +      |      |           |
| ["classrooms", CLASSROOM_ID, "events"]       |        |      |        |      | +         |
