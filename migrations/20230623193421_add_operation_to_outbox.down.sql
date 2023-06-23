alter table outbox
    drop constraint outbox_pkey,
    add primary key (entity_type, id),
    drop column operation;
