CREATE TABLE IF NOT EXISTS ban_account (
    classroom_id uuid NOT NULL,
    target account_id NOT NULL,
    created_at timestamp with time zone DEFAULT NOW() NOT NULL,

    PRIMARY KEY (classroom_id, target)
);
