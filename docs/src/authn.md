# Authentication

To authenticate client should send the `Authorization` header with the `Bearer <token>` payload. This token proves that the user has some Account Id.

Above is a standard authentication scheme used in all applications.

In addition, Conference requires the user to provide some Agent label using the `X-Agent-Label` header with a label as a value.