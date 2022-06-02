CREATE TYPE graph_execution_status AS ENUM ('WAITING', 'EXECUTING', 'COMPLETED', 'SCHEDULED_TO_FAIL', 'FAILED');

CREATE TABLE graph_execution_state
(
    workflow_id varchar(255),
    id varchar(255),
    error_description varchar(1023) NULL,
    status graph_execution_status default 'WAITING',

    graph_description_json varchar(10485760),
    task_executions_json varchar(10485760),
    current_execution_group_json varchar(10485760),

    last_updated timestamp,

    primary key (workflow_id, id)
);