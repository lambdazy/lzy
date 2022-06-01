CREATE TYPE graph_execution_status AS ENUM ('WAITING', 'EXECUTING', 'COMPLETED', 'SCHEDULED_TO_FAIL', 'FAILED');

CREATE TABLE graph_execution_state
(
    workflow_id varchar(255),
    id varchar(255),
    error_description varchar(255) NULL,
    status graph_execution_status default 'WAITING',

    graph_description_json varchar(255),
    task_executions_json varchar(255),
    current_execution_group_json varchar(255),

    primary key (workflow_id, id)
);