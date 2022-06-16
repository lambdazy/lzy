CREATE TYPE graph_execution_status AS ENUM ('WAITING', 'EXECUTING', 'COMPLETED', 'FAILED');

CREATE TABLE graph_execution_state
(
    workflow_id varchar(255) NOT NULL,
    id varchar(255) NOT NULL,
    error_description varchar(1023) NULL,
    status graph_execution_status default 'WAITING' NOT NULL,

    graph_description_json varchar(10485760) NOT NULL,
    task_executions_json varchar(10485760) NOT NULL,
    current_execution_group_json varchar(10485760) NOT NULL,

    last_updated timestamp NOT NULL,
    acquired bool NOT NULL,

    primary key (workflow_id, id)
);

CREATE TYPE event_type AS ENUM ('START', 'STOP');

CREATE TABLE queue_event
(
    id varchar(255) NOT NULL PRIMARY KEY,

    type event_type NOT NULL,
    workflow_id varchar(255) NOT NULL,
    graph_id varchar(255) NOT NULL,
    acquired bool NOT NULL,
    description varchar(10485760) NOT NULL,

    foreign key (workflow_id, graph_id) REFERENCES graph_execution_state (workflow_id, id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
)