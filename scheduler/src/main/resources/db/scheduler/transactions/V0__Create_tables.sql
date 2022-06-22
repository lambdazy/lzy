CREATE type servant_event_type AS ENUM (
    'ALLOCATION_REQUESTED',
    'ALLOCATION_TIMEOUT',
    'CONNECTED',
    'CONFIGURED',
    'CONFIGURATION_TIMEOUT',
    'EXECUTION_REQUESTED',
    'EXECUTION_COMPLETED',
    'EXECUTION_TIMEOUT',
    'DISCONNECTED',
    'COMMUNICATION_COMPLETED',
    'IDLE_TIMEOUT',
    'SIGNAL',
    'STOP',
    'STOPPING_TIMEOUT',
    'STOPPED'
);

CREATE type servant_status AS ENUM (
    'CREATED',
    'CONNECTING',
    'CONFIGURING',
    'IDLE',
    'RUNNING',
    'EXECUTING',
    'STOPPING',
    'DESTROYED'
);

CREATE TABLE servant (
    id varchar(255) NOT NULL,
    workflow_id varchar(255) NOT NULL,
    status servant_status NOT NULL,
    provisioning varchar(64) ARRAY NOT NULL,
    env_json varchar(10485760) NOT NULL,

    error_description varchar(2048) NULL,
    task_id varchar(255) NULL,

    PRIMARY KEY (id, workflow_id)
);

CREATE TABLE servant_event (
    id varchar(255) NOT NULL PRIMARY KEY,
    time timestamp NOT NULL,
    servant_id varchar(255) NOT NULL,
    workflow_id varchar(255) NOT NULL,
    type servant_event_type NOT NULL,

    description varchar(10485760) NULL,
    rc int NULL,
    task_id varchar(255) NULL,
    signal_num int NULL,

    FOREIGN KEY (servant_id, workflow_id) references servants(servant_id, workflow_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FOREIGN KEY (task_id, workflow_id) references tasks(task_id, workflow_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);