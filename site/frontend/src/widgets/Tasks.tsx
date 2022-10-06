import axios from "axios";
import {useContext, useEffect, useState} from "react";
import {BACKEND_HOST} from "../config";
import {AuthContext, UserCredentials} from "../logic/Auth";
import {ErrorAlert, useAlert} from "./ErrorAlert";
import {DataGrid, GridColDef, GridRowsProp} from '@mui/x-data-grid';
import {Redirect} from "react-router-dom";
import {TextField} from "@mui/material";

export interface Task {
    taskId: string;
    operationName: string;
    status: string;
    description: string;
}

async function fetchTasks(credentials: UserCredentials, workflowId: string): Promise<Task[]> {
    const res = await axios.post(BACKEND_HOST() + "/tasks/get", {
        credentials, workflowId
    })
    return res.data.taskStatusList;
}

interface ToolbarProps {
    taskUpdater: (tasks: Task[]) => void;
}

function Toolbar(props: ToolbarProps) {
    let {userCreds} = useContext(AuthContext);
    let alert = useAlert();
    let [workflowId, setWorkflowId] = useState<string>();

    useEffect(() => {
        if (userCreds && workflowId) {
            fetchTasks(userCreds, workflowId)
                .then((tasks) => {
                    if (tasks === undefined) {
                        tasks = [];
                    }
                    props.taskUpdater(tasks);
                })
                .catch((error) => {
                    alert.showDanger("Error while fetching tasks", error.message);
                })
        }
    }, [workflowId])

    if (userCreds == null) {
        alert.showDanger("Error", "You are not logged in");
        return <Redirect to="/login" />;
    }

    return <div>
        <TextField
            autoFocus
            margin="dense"
            id="name"
            label="WorkflowId"
            fullWidth
            variant="standard"
            onChange={e => setWorkflowId(e.target.value)}
        />
    </div>
}

export function Tasks() {
    let [tasks, setTasks] = useState<Task[]>();

    const columns: GridColDef[] = [
        {field: 'id', headerName: 'Task id', width: 250},
        {field: 'operationName', headerName: 'Operation name', width: 250},
        {field: 'status', headerName: 'Status', width: 150},
        {field: 'description', headerName: 'Task description', width: 350}
    ];

    let rows: GridRowsProp = tasks ? tasks.map((task) => {
        return {
            id: task.taskId,
            operationName: task.operationName,
            status: task.status,
            description: task.description
        }
    }) : [];

    return <div>
        <DataGrid
            autoHeight
            columns={columns}
            rows={rows}
            components={{Toolbar}}
            componentsProps={{
                toolbar: {taskUpdater: (tasks: Task[]) => setTasks(tasks)}
            }}
        />
    </div>
}