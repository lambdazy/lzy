CREATE type worker_event_type AS ENUM (
    'NOOP',
    'ALLOCATION_TIMEOUT',
    'CONNECTED',
    'CONFIGURED',
    'CONFIGURATION_TIMEOUT',
    'EXECUTION_REQUESTED',
    'EXECUTING_HEARTBEAT',
    'EXECUTING_HEARTBEAT_TIMEOUT',
    'EXECUTION_COMPLETED',
    'EXECUTION_TIMEOUT',
    'COMMUNICATION_COMPLETED',
    'IDLE_HEARTBEAT',
    'IDLE_HEARTBEAT_TIMEOUT',
    'IDLE_TIMEOUT',
    'STOP',
    'STOPPING_TIMEOUT',
    'STOPPED'
);

CREATE type worker_status AS ENUM (
    'CREATED',
    'CONNECTING',
    'CONFIGURING',
    'IDLE',
    'RUNNING',
    'EXECUTING',
    'STOPPING',
    'DESTROYED'
);

CREATE TABLE worker (
    id text NOT NULL,
    user_id text NOT NULL,
    workflow_name text NOT NULL,
    status worker_status NOT NULL,
    requirements_json text NOT NULL,

    error_description text NULL,
    task_id text NULL,
    worker_url text NULL,

    allocator_meta text NULL,

    acquired bool NOT NULL DEFAULT false,
    acquired_for_task bool NOT NULL DEFAULT false,

    PRIMARY KEY (id, workflow_name)
);

CREATE TABLE worker_event (
    id text NOT NULL PRIMARY KEY,
    time timestamp NOT NULL,
    worker_id text NOT NULL,
    workflow_name text NOT NULL,
    type worker_event_type NOT NULL,

    description text NULL,
    rc int NULL,
    task_id text NULL,
    worker_url text NULL,

    FOREIGN KEY (worker_id, workflow_name) references worker(id, workflow_name)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE type task_status AS ENUM (
    'QUEUE',
    'SCHEDULED',
    'EXECUTING',
    'SUCCESS',
    'ERROR'
);

CREATE TABLE task (
    id text NOT NULL,
    workflow_id text NOT NULL,
    workflow_name text NOT NULL,
    user_id text NOT NULL,
    task_description_json text NOT NULL,
    status task_status NOT NULL,

    rc int NULL,
    error_description text NULL,
    worker_id text NULL,
    PRIMARY KEY(id, workflow_id)
);