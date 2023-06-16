CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'account_id') THEN
        CREATE TYPE account_id AS (
            label text,
            audience text
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_connection_status') THEN
        CREATE TYPE agent_connection_status AS ENUM (
            'in_progress',
            'connected'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_id') THEN
        CREATE TYPE agent_id AS (
            account_id account_id,
            label text
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_status') THEN
        CREATE TYPE agent_status AS ENUM (
            'in_progress',
            'ready'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'recording_status') THEN
        CREATE TYPE recording_status AS ENUM (
            'in_progress',
            'ready',
            'missing'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'room_backend') THEN
        CREATE TYPE room_backend AS ENUM (
            'none',
            'janus'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'rtc_sharing_policy') THEN
        CREATE TYPE rtc_sharing_policy AS ENUM (
            'none',
            'shared',
            'owned'
        );
    END IF;
END $$;

CREATE OR REPLACE FUNCTION on_rtc_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    rtc_sharing_policy rtc_sharing_policy;
BEGIN
    CASE (SELECT r.rtc_sharing_policy FROM room AS r WHERE id = NEW.room_id)
    WHEN 'none' THEN
        -- RTC creation not allowed.
        RAISE EXCEPTION 'creating RTC within a room with `none` RTC sharing policy is not allowed';
    WHEN 'shared' THEN
        -- Only single RTC allowed in the room.
        IF (SELECT COUNT(id) FROM rtc WHERE room_id = NEW.room_id) = 0 THEN
            RETURN NEW;
        ELSE
            RAISE EXCEPTION 'creating multiple RTCs within a room with `shared` RTC sharing policy is not allowed';
        END IF;
    WHEN 'owned' THEN
        -- Only single RTC per agent allowed in the room.
        IF (SELECT COUNT(id) FROM rtc WHERE room_id = NEW.room_id AND created_by = NEW.created_by) = 0 THEN
            RETURN NEW;
        ELSE
            RAISE EXCEPTION 'creating multiple RTCs per agent within a room with `owned` RTC sharing policy is not allowed';
        END IF;
    END CASE;
END;
$$;

CREATE OR REPLACE FUNCTION trigger_set_timestamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS _sqlx_migrations (
    version bigint NOT NULL,
    description text NOT NULL,
    installed_on timestamp with time zone DEFAULT now() NOT NULL,
    success boolean NOT NULL,
    checksum bytea NOT NULL,
    execution_time bigint NOT NULL,

    PRIMARY KEY (version)
);

CREATE TABLE IF NOT EXISTS room (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    "time" tstzrange NOT NULL,
    audience text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    backend room_backend DEFAULT 'none'::room_backend NOT NULL,
    reserve integer,
    tags json DEFAULT '{}'::json NOT NULL,
    backend_id agent_id,
    rtc_sharing_policy rtc_sharing_policy DEFAULT 'none'::rtc_sharing_policy NOT NULL,
    classroom_id uuid NOT NULL,
    host agent_id,
    timed_out boolean DEFAULT false NOT NULL,
    closed_by agent_id,
    infinite boolean DEFAULT false NOT NULL,
    CONSTRAINT room_backend_id_check CHECK (((backend_id IS NULL) OR (rtc_sharing_policy <> 'none'::rtc_sharing_policy))),
    CONSTRAINT room_time_presence CHECK (("time" <> 'empty'::tstzrange)),

    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS rtc (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    room_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by agent_id NOT NULL,

    FOREIGN KEY (room_id) REFERENCES room (id) ON DELETE CASCADE,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recording (
    rtc_id uuid NOT NULL,
    started_at timestamp with time zone,
    segments int8range[],
    status recording_status DEFAULT 'in_progress'::recording_status NOT NULL,
    mjr_dumps_uris text[],

    FOREIGN KEY (rtc_id) REFERENCES rtc (id) ON DELETE CASCADE,
    PRIMARY KEY (rtc_id)
);

CREATE TABLE IF NOT EXISTS janus_backend (
    id agent_id NOT NULL,
    handle_id bigint NOT NULL,
    session_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    capacity integer,
    balancer_capacity integer,
    api_version text DEFAULT 'v1'::text NOT NULL,
    "group" text,
    janus_url text NOT NULL,

    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS janus_rtc_stream (
    id uuid NOT NULL,
    handle_id bigint NOT NULL,
    rtc_id uuid NOT NULL,
    backend_id agent_id NOT NULL,
    label text NOT NULL,
    sent_by agent_id NOT NULL,
    "time" tstzrange,
    created_at timestamp with time zone DEFAULT now() NOT NULL,

    CONSTRAINT janus_rtc_stream_sent_by_check CHECK (((sent_by).label IS NOT NULL)),
    CONSTRAINT janus_rtc_stream_sent_by_check1 CHECK (((sent_by).account_id.label IS NOT NULL)),
    CONSTRAINT janus_rtc_stream_sent_by_check2 CHECK (((sent_by).account_id.audience IS NOT NULL)),

    FOREIGN KEY (rtc_id) REFERENCES rtc (id) ON DELETE CASCADE,
    FOREIGN KEY (backend_id) REFERENCES janus_backend (id) ON DELETE CASCADE,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS agent (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    agent_id agent_id NOT NULL,
    room_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    status agent_status DEFAULT 'in_progress'::agent_status NOT NULL,

    FOREIGN KEY (room_id) REFERENCES room (id) ON DELETE CASCADE,
    UNIQUE (agent_id, room_id),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS agent_connection (
    agent_id uuid NOT NULL,
    handle_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    rtc_id uuid NOT NULL,
    status agent_connection_status DEFAULT 'in_progress'::agent_connection_status NOT NULL,

    FOREIGN KEY (agent_id) REFERENCES agent (id) ON DELETE CASCADE,
    FOREIGN KEY (rtc_id) REFERENCES rtc (id) ON DELETE CASCADE,
    PRIMARY KEY (agent_id, rtc_id)
);

CREATE TABLE IF NOT EXISTS group_agent (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    room_id uuid NOT NULL,
    groups jsonb DEFAULT '[]'::jsonb NOT NULL,

    FOREIGN KEY (room_id) REFERENCES room (id) ON DELETE CASCADE,
    UNIQUE (room_id),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS orphaned_room (
    id uuid NOT NULL,
    host_left_at timestamp with time zone NOT NULL,

    FOREIGN KEY (id) REFERENCES room (id),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS outbox (
    id bigint NOT NULL,
    entity_type text NOT NULL,
    stage jsonb NOT NULL,
    delivery_deadline_at timestamp with time zone NOT NULL,
    error_kind text,
    retry_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,

    PRIMARY KEY (entity_type, id)
);

CREATE SEQUENCE IF NOT EXISTS outbox_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE outbox_id_seq OWNED BY outbox.id;

CREATE TABLE IF NOT EXISTS rtc_reader_config (
    rtc_id uuid NOT NULL,
    reader_id agent_id NOT NULL,
    receive_video boolean NOT NULL,
    receive_audio boolean NOT NULL,

    FOREIGN KEY (rtc_id) REFERENCES rtc (id) ON DELETE CASCADE,
    PRIMARY KEY (rtc_id, reader_id)
);

CREATE TABLE IF NOT EXISTS rtc_writer_config (
    rtc_id uuid NOT NULL,
    send_video boolean NOT NULL,
    send_audio boolean NOT NULL,
    video_remb bigint,
    send_audio_updated_by agent_id,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT rtc_writer_config_video_remb_check CHECK (((video_remb IS NULL) OR (video_remb > 0))),

    PRIMARY KEY (rtc_id),
    FOREIGN KEY (rtc_id) REFERENCES rtc (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS rtc_writer_config_snapshot (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    rtc_id uuid NOT NULL,
    send_video boolean,
    send_audio boolean,
    created_at timestamp with time zone DEFAULT now() NOT NULL,

    PRIMARY KEY (id),
    FOREIGN KEY (rtc_id) REFERENCES rtc (id) ON DELETE CASCADE
);

ALTER TABLE ONLY outbox ALTER COLUMN id SET DEFAULT nextval('outbox_id_seq'::regclass);

CREATE INDEX IF NOT EXISTS orphaned_room_host_left_at ON orphaned_room USING btree (host_left_at);
CREATE UNIQUE INDEX IF NOT EXISTS recording_rtc_id_idx ON recording USING btree (rtc_id);
CREATE INDEX IF NOT EXISTS room_time ON room USING gist ("time") WHERE (backend_id IS NOT NULL);
CREATE INDEX IF NOT EXISTS rtc_room_id ON rtc USING btree (room_id);

CREATE OR REPLACE TRIGGER rtc_insert_trigger BEFORE INSERT ON rtc FOR EACH ROW EXECUTE FUNCTION on_rtc_insert();
CREATE OR REPLACE TRIGGER set_timestamp BEFORE UPDATE ON rtc_writer_config FOR EACH ROW EXECUTE FUNCTION trigger_set_timestamp();
