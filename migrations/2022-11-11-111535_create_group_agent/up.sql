create table group_agent (
    id uuid default gen_random_uuid(),
    group_id uuid not null,
    agent_id agent_id not null,
    
    foreign key (group_id) references "group" (id) on delete cascade,
    unique (group_id, agent_id),
    primary key (id)
);
