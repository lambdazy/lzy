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
    id varchar(255) NOT NULL,
    user_id varchar(255) NOT NULL,
    workflow_name varchar(255) NOT NULL,
    status worker_status NOT NULL,
    requirements_json varchar(10485760) NOT NULL,

    error_description varchar(2048) NULL,
    task_id varchar(255) NULL,
    worker_url varchar(255) NULL,

    allocator_meta varchar(10485760) NULL,

    acquired bool NOT NULL DEFAULT false,
    acquired_for_task bool NOT NULL DEFAULT false,

    PRIMARY KEY (id, workflow_name)
);

CREATE TABLE worker_event (
    id varchar(255) NOT NULL PRIMARY KEY,
    time timestamp NOT NULL,
    worker_id varchar(255) NOT NULL,
    workflow_name varchar(255) NOT NULL,
    type worker_event_type NOT NULL,

    description varchar(10485760) NULL,
    rc int NULL,
    task_id varchar(255) NULL,
    worker_url varchar(255) NULL,

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
    id varchar(255) NOT NULL,
    workflow_id varchar(255) NOT NULL,
    workflow_name varchar(255) NOT NULL,
    user_id varchar(255) NOT NULL,
    task_description_json varchar(10485760) NOT NULL,
    status task_status NOT NULL,

    rc int NULL,
    error_description varchar(10485760) NULL,
    worker_id varchar(255) NULL,
    PRIMARY KEY(id, workflow_id)
);