import axios from "axios";
import { useState } from "react";
import { useAsync } from "react-async";
import { BACKEND_HOST } from "../config";
import { useAuth, UserCredentials } from "../logic/Auth";
import { useAlert } from "./ErrorAlert";
import { Header } from "./Header";
import { DataGrid, GridCellParams, GridColDef, GridRowsProp, MuiEvent } from '@mui/x-data-grid';

export interface Task{
    taskId: string;
    owner: string;
    servant: string;
    explanation: string;
    status: string;
    fuse: string;
    tags: string[];
}

async function fetchTasks(credentials: UserCredentials): Promise<Task[]>{
    const res = await axios.post(BACKEND_HOST()+"/tasks/get", {
        credentials
    })
    return res.data.tasks;
}

export function TasksInternal(props: {}){
    let auth = useAuth();
    let alert = useAlert();
    let {data, error} = useAsync({promiseFn: auth.getCredentials});
    let [tasks, setTasks] = useState<Task[]>();
    let [openPopper, setOpenPopper] = useState<{show: boolean, text: string}>({show: false, text: ""});
    if (error){
        alert.show(error.message, error.name, () => {}, "danger");
    };
    if (data !== undefined){
        if (data == null){
            alert.show("Not logged in", "Error", () => {}, "danger");
        }
        else if (tasks === undefined){
            fetchTasks(data)
            .then((tasks) => {
                if (tasks === undefined){
                    tasks = [];
                }
                setTasks(tasks);
            })
            .catch((error) => {alert.show(error.message, "Error while fetching tasks", () => {}, "danger");})
        }
    }
    const columns: GridColDef[] = [
        { field: 'id', headerName: 'Task id', width: 250 },
        { field: 'description', headerName: 'Task description', width: 350 },
        { field: 'status', headerName: 'Status', width: 150 },
        { field: 'tags', headerName: 'Servant tags', width: 150 },
    ];

    let rows: GridRowsProp = tasks ? tasks.map((task) => {return {
        id: task.taskId, status: task.status, fuse: task.fuse, tags: task.tags === undefined ? "No tags" : task.tags.join("\n")
    }}) : [];
    console.log(rows);

    return (
        <>
        <DataGrid autoHeight  columns={columns} rows={rows}/>
        </>
    )


}

export function Tasks(props: {}){
    return(<> 
        <Header />
        <TasksInternal />
    </>)
}