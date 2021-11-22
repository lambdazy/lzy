import axios from "axios";
import { useState } from "react";
import { useAsync } from "react-async";
import { Col,Container, Row } from "react-bootstrap";
import { BACKEND_HOST } from "../config";
import { useAuth, UserCredentials } from "../logic/Auth";
import { useAlert } from "./ErrorAlert";
import { Header } from "./Header";
import { DataGrid, GridColDef, GridRowsProp } from '@mui/x-data-grid';

export interface Task{
    taskId: string;
    owner: string;
    servant: string;
    explanation: string;
    status: string;
}

function generateTasks(tasks: Task[]){
    return Array.from(tasks.map((t) =>
        <Row>
            <Col>{t.taskId}</Col>
            <Col>{t.status}</Col>
        </Row>
    ))
}

async function fetchTasks(credentials: UserCredentials): Promise<Task[]>{
    const res = await axios.post(BACKEND_HOST()+"/tasks/get", {
        credentials
    })
    return res.data.tasks;
}

export function Tasks(props: {}){
    let auth = useAuth();
    let alert = useAlert();
    let {data, error} = useAsync({promiseFn: auth.getCredentials});
    let [tasks, setTasks] = useState<Task[]>();
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
        { field: 'status', headerName: 'Status', width: 150 },
    ];

    let rows: GridRowsProp = tasks ? tasks.map((task) => {return {id: task.taskId, status: task.status}}) : [];
    console.log(rows);

    return (
        <>
        <Header />
        <Container>
            <DataGrid columns={columns} rows={rows} />
        </Container>
        </>
    )


}