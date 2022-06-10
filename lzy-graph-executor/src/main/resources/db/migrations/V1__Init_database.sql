CREATE TYPE graph_execution_status AS ENUM ('WAITING', 'EXECUTING', 'COMPLETED', 'SCHEDULED_TO_FAIL', 'FAILED');

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
    acquired_before timestamp NULL,

    primary key (workflow_id, id)
);