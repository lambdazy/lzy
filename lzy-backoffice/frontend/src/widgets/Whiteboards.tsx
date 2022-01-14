import {useState} from "react";
import {useAsync} from "react-async";
import {useAuth, UserCredentials} from "../logic/Auth";
import {useAlert} from "./ErrorAlert";
import {Header} from "./Header";
import {DataGrid, GridColDef, GridRowsProp} from '@mui/x-data-grid';
import {BACKEND_HOST} from "../config";
import axios from "axios";

export interface Whiteboard {
    wbId: string;
    wbStatus: string;
}

async function fetchWhiteboards(credentials: UserCredentials): Promise<Whiteboard[]>{
    const res = await axios.post(BACKEND_HOST()+"/whiteboards/get", {
        credentials
    }).then(response => {
        console.log(response.data);
        return response
    });
    return res.data.whiteboardsInfo;
}

export function WhiteboardsInternal(props: {}){
    let auth = useAuth();
    let alert = useAlert();
    let {data, error} = useAsync({promiseFn: auth.getCredentials});
    let [wbInfos, setWbInfos] = useState<Whiteboard[]>();
    if (error){
        alert.show(error.message, error.name, () => {}, "danger");
    }
    if (data !== undefined){
        if (data == null){
            alert.show("Not logged in", "Error", () => {}, "danger");
        }
        else if (wbInfos === undefined) {
            fetchWhiteboards(data)
                .then((wbInfos) => {
                    if (wbInfos === undefined){
                        wbInfos = [];
                    }
                    setWbInfos(wbInfos);
                })
                .catch((error) => {alert.show(error.message, "Error while fetching tasks", () => {}, "danger");})
        }
    }
    const columns: GridColDef[] = [
        { field: 'id', headerName: 'Whiteboard id', width: 350 },
        { field: 'status', headerName: 'Status', width: 150 },
    ];

    let rows: GridRowsProp = wbInfos ? wbInfos.map((wbInfo) => {return {
        id: wbInfo.wbId, status: wbInfo.wbStatus
    }}) : [];
    console.log(rows);

    return (
        <>
            <DataGrid autoHeight  columns={columns} rows={rows}/>
        </>
    )


}

export function Whiteboards(props: {}){
    return(<>
        <Header />
        <WhiteboardsInternal />
    </>)
}