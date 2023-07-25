alter table outbox
    add operation text not null,
    drop constraint outbox_pkey,
    add primary key (entity_type, operation, id);
