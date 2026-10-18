-- The initial schema. See docs/design/schema.md for the reasoning behind every
-- table and index. __SCHEMA__ is replaced with the configured schema name.

CREATE TYPE __SCHEMA__.task_state AS ENUM (
    'scheduled', 'pending', 'available', 'running',
    'retryable', 'completed', 'dead', 'canceled'
);

CREATE TABLE __SCHEMA__.tasks (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_at              timestamptz NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    deadline            timestamptz,
    lease_expires_at    timestamptz,
    attempted_at        timestamptz,
    cancel_requested_at timestamptz,
    finalized_at        timestamptz,
    lease_token         integer NOT NULL DEFAULT 0,
    state               __SCHEMA__.task_state NOT NULL,
    priority            smallint NOT NULL DEFAULT 2,
    attempt             smallint NOT NULL DEFAULT 0,
    max_attempts        smallint NOT NULL,
    queue               text NOT NULL,
    kind                text NOT NULL,
    group_key           text,
    attempted_by        text,
    payload             jsonb NOT NULL,
    metadata            jsonb NOT NULL DEFAULT '{}',
    errors              jsonb NOT NULL DEFAULT '[]',

    CONSTRAINT tasks_queue_check     CHECK (char_length(queue) BETWEEN 1 AND 128),
    CONSTRAINT tasks_kind_check      CHECK (char_length(kind) BETWEEN 1 AND 128),
    CONSTRAINT tasks_group_key_check CHECK (char_length(group_key) BETWEEN 1 AND 256),
    CONSTRAINT tasks_attempts_check  CHECK (max_attempts >= 1 AND attempt BETWEEN 0 AND max_attempts),
    CONSTRAINT tasks_state_check     CHECK (state <> 'completed'),
    CONSTRAINT tasks_pending_check   CHECK (state <> 'pending' OR group_key IS NOT NULL),
    CONSTRAINT tasks_lease_check     CHECK ((state = 'running') = (lease_expires_at IS NOT NULL)),
    CONSTRAINT tasks_finalized_check CHECK ((state IN ('dead', 'canceled')) = (finalized_at IS NOT NULL))
) WITH (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_cost_delay = 0
);

CREATE INDEX tasks_fetch_idx    ON __SCHEMA__.tasks (queue, priority DESC, run_at, id) WHERE state = 'available';
CREATE INDEX tasks_due_idx      ON __SCHEMA__.tasks (run_at) WHERE state IN ('scheduled', 'retryable');
CREATE INDEX tasks_running_idx  ON __SCHEMA__.tasks (queue) WHERE state = 'running';
CREATE INDEX tasks_pending_idx  ON __SCHEMA__.tasks (queue, group_key, id) WHERE state = 'pending';
CREATE INDEX tasks_deadline_idx ON __SCHEMA__.tasks (deadline)
    WHERE deadline IS NOT NULL AND state IN ('scheduled', 'pending', 'available', 'retryable');
CREATE INDEX tasks_finished_idx ON __SCHEMA__.tasks (queue, finalized_at) WHERE state IN ('dead', 'canceled');

CREATE TABLE __SCHEMA__.task_history (
    id           bigint NOT NULL,
    finalized_at timestamptz NOT NULL,
    created_at   timestamptz NOT NULL,
    run_at       timestamptz NOT NULL,
    attempted_at timestamptz NOT NULL,
    priority     smallint NOT NULL,
    attempt      smallint NOT NULL,
    max_attempts smallint NOT NULL,
    queue        text NOT NULL,
    kind         text NOT NULL,
    group_key    text,
    attempted_by text NOT NULL,
    payload      jsonb NOT NULL,
    metadata     jsonb NOT NULL,
    errors       jsonb NOT NULL
) PARTITION BY RANGE (finalized_at);

CREATE INDEX task_history_queue_idx ON __SCHEMA__.task_history (queue, finalized_at);
CREATE INDEX task_history_id_idx    ON __SCHEMA__.task_history (id);

CREATE TABLE __SCHEMA__.groups (
    queue          text NOT NULL,
    group_key      text NOT NULL,
    tasks          integer NOT NULL,
    halted_task_id bigint,
    halted_at      timestamptz,
    updated_at     timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (queue, group_key),
    CONSTRAINT groups_tasks_check  CHECK (tasks >= 0),
    CONSTRAINT groups_halted_check CHECK ((halted_task_id IS NULL) = (halted_at IS NULL))
) WITH (fillfactor = 80);

CREATE INDEX groups_halted_idx ON __SCHEMA__.groups (queue) WHERE halted_at IS NOT NULL;

CREATE TABLE __SCHEMA__.dedupe_keys (
    key        bytea PRIMARY KEY,
    task_id    bigint NOT NULL,
    expires_at timestamptz NOT NULL,

    CONSTRAINT dedupe_keys_key_check CHECK (octet_length(key) = 32)
);

CREATE INDEX dedupe_keys_expires_idx ON __SCHEMA__.dedupe_keys (expires_at);

CREATE TABLE __SCHEMA__.queues (
    name               text PRIMARY KEY,
    paused_at          timestamptz,
    max_running        integer,
    keep_history       boolean NOT NULL DEFAULT true,
    halt_group_on_dead boolean NOT NULL DEFAULT false,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT queues_name_check        CHECK (char_length(name) BETWEEN 1 AND 128),
    CONSTRAINT queues_max_running_check CHECK (max_running > 0)
);

CREATE TABLE __SCHEMA__.settings (
    id                boolean PRIMARY KEY DEFAULT true,
    history_retention interval NOT NULL DEFAULT '7 days',
    dead_retention    interval NOT NULL DEFAULT '30 days',
    updated_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT settings_single_row_check CHECK (id)
);

INSERT INTO __SCHEMA__.settings DEFAULT VALUES;

CREATE TABLE __SCHEMA__.schedules (
    name        text PRIMARY KEY,
    queue       text NOT NULL,
    kind        text NOT NULL,
    cron        text NOT NULL,
    timezone    text NOT NULL DEFAULT 'UTC',
    payload     jsonb NOT NULL DEFAULT '{}',
    next_run_at timestamptz NOT NULL,
    last_run_at timestamptz,
    paused_at   timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX schedules_due_idx ON __SCHEMA__.schedules (next_run_at) WHERE paused_at IS NULL;

CREATE TABLE __SCHEMA__.workers (
    id           text PRIMARY KEY,
    queues       text[] NOT NULL,
    hostname     text NOT NULL,
    version      text NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    metadata     jsonb NOT NULL DEFAULT '{}'
) WITH (fillfactor = 50);

CREATE TABLE __SCHEMA__.leader (
    name       text PRIMARY KEY,
    worker_id  text NOT NULL,
    elected_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
);

CREATE TABLE __SCHEMA__.queue_stats (
    queue        text NOT NULL,
    state        __SCHEMA__.task_state NOT NULL,
    tasks        bigint NOT NULL,
    capped       boolean NOT NULL,
    oldest_at    timestamptz,
    refreshed_at timestamptz NOT NULL,

    PRIMARY KEY (queue, state)
);
