import axios from "axios";
import {useContext, useState} from "react";
import {BACKEND_HOST} from "../config";
import {AuthContext, UserCredentials} from "../logic/Auth";
import {useAlert} from "./ErrorAlert";
import {DataGrid, GridColDef, GridRowsProp} from '@mui/x-data-grid';

export interface Task {
    taskId: string;
    owner: string;
    servant: string;
    explanation: string;
    status: string;
    fuse: string;
    tags: string[];
}

async function fetchTasks(credentials: UserCredentials): Promise<Task[]> {
    const res = await axios.post(BACKEND_HOST() + "/tasks/get", {
        credentials
    })
    return res.data.tasks;
}

export function TasksInternal() {
    let {userCreds} = useContext(AuthContext);
    let alert = useAlert();

    let [tasks, setTasks] = useState<Task[]>();

    if (userCreds == null) {
        alert.showDanger("Error", "You are not logged in");
    } else if (tasks === undefined) {
        fetchTasks(userCreds)
            .then((tasks) => {
                if (tasks === undefined) {
                    tasks = [];
                }
                setTasks(tasks);
            })
            .catch((error) => {
                alert.showDanger("Error while fetching tasks", error.message);
            })
    }

    const columns: GridColDef[] = [
        {field: 'id', headerName: 'Task id', width: 250},
        {field: 'description', headerName: 'Task description', width: 350},
        {field: 'status', headerName: 'Status', width: 150},
        {field: 'tags', headerName: 'Servant tags', width: 150},
    ];

    let rows: GridRowsProp = tasks ? tasks.map((task) => {
        return {
            id: task.taskId,
            status: task.status,
            fuse: task.fuse,
            tags: task.tags === undefined ? "No tags" : task.tags.join("\n")
        }
    }) : [];
    console.log(rows);

    return (
        <>
            <DataGrid autoHeight columns={columns} rows={rows}/>
        </>
    )
}

export function Tasks() {
    return (<>
        <TasksInternal/>
    </>)
}