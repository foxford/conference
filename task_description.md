# Описание задачи

Из src/app/endpoint/group/update.rs и src/app/endpoint/room.rs после обработки и валидации входных параметров
происходит отправка следующего события в NATS:

```rust
// https://github.com/foxford/svc-events/pull/32/files#diff-b58cd6d24cdb79985643dea3040c80f136156eb27abea76384ac3bc8e2cea7d7R60
pub enum VideoGroupIntentEventV1 {
    CreateIntent {
        backend_id: AgentId,
        created_at: i64,
    },  
    UpdateIntent {
        backend_id: AgentId,
        created_at: i64,
    },  
    DeleteIntent {
        backend_id: AgentId,
        created_at: i64,
    },  
}
```
(из room.rs только `UpdateIntent`)

Это событие обрабатывает сам conference в src/app/stage/mod.rs (https://github.com/foxford/conference/pull/429/files#diff-377b84b48e75e8891aabfb18cc32878b222ab4bf58532528d7147874b09335e4R94).
В этом обработчике выполняется:
1. обновление конфигурации в базе
2. обновление конфигурации в Janus
3. отправка нотификации в NATS, для этого преобразуем исходное `VideoGroupIntentEventV1` в `VideoGroupEventV1`
4. отправка нотификации в VerneMQ


Вопросы:
1. `created_at` в `VideoGroupEventV1` указывает время получения запроса на endpoint или время создания этого события?

Предложение:
1. Перенесенти переименование `VideoGroup` в следующую задачу (https://github.com/foxford/svc-events/blob/main/src/v1/mod.rs#L12).