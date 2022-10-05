// import {Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle, TextField} from "@mui/material";
import {DataGrid, GridColDef, GridRowsProp, GridSelectionModel} from "@mui/x-data-grid";
import axios from "axios";
import {useContext, useEffect, useState} from "react";
import {BACKEND_HOST} from "../config";
import {AuthContext, UserCredentials} from "../logic/Auth"
import {useAlert} from "./ErrorAlert";

interface Token {
    name: string;
}

async function getTokens(credentials: UserCredentials | undefined | null): Promise<Token[]> {
    if (!credentials)
        return [];
    return (await axios.post(
        BACKEND_HOST() + "/public_key/list",
        {credentials: credentials}
    )).data.keyNames;
}

interface State {
    keys: Token[] | undefined
}

// interface ToolbarProps {
//     selectionModel: GridSelectionModel,
//     credentials: UserCredentials,
//     update: () => void
// }

// function Toolbar(props: ToolbarProps) {
//     const {selectionModel, credentials, update} = props;
//
//     const handleClick = () => {
//         if (!selectionModel) {
//             return;
//         }
//         selectionModel.forEach((param: string | number) => {
//             axios.post(BACKEND_HOST() + "/public_key/delete", {keyName: param, credentials})
//                 .then(() => update())
//         });
//     };
//
//     const handleMouseDown = (event: { preventDefault: () => void; }) => {
//         // Keep the focus in the cell
//         event.preventDefault();
//     };
//
//     let [open, setOpen] = useState<boolean>(false);
//     let [keyData, setKeyData] = useState<{ name: string, value: string }>({name: "", value: ""});
//
//     const handleAddKey = () => {
//         axios.post(BACKEND_HOST() + "/public_key/add", {
//             keyName: keyData.name,
//             publicKey: keyData.value,
//             userCredentials: credentials
//         })
//             .then(() => {
//                 setOpen(false);
//                 update()
//             })
//     }
//
//     return (
//         <div>
//             <Button
//                 onClick={handleClick}
//                 onMouseDown={handleMouseDown}
//                 disabled={!selectionModel || selectionModel.length == 0}
//                 color="primary"
//             >
//                 Delete
//             </Button>
//             <Button onClick={() => {
//                 setOpen(true)
//             }}>
//                 Add
//             </Button>
//             <Dialog open={open}>
//                 <DialogTitle>Add key</DialogTitle>
//                 <DialogContent>
//                     <DialogContentText>
//                         Add new public key to server.
//                     </DialogContentText>
//                     <TextField
//                         autoFocus
//                         margin="dense"
//                         id="name"
//                         label="Key name"
//                         fullWidth
//                         variant="standard"
//                         onChange={(event) => {
//                             setKeyData({name: event.target.value, value: keyData.value})
//                         }}
//                     />
//                     <TextField
//                         margin="dense"
//                         id="value"
//                         label="Key value"
//                         fullWidth
//                         multiline
//                         variant="standard"
//                         onChange={(event) => {
//                             setKeyData({name: keyData.name, value: event.target.value})
//                         }}
//                     />
//                 </DialogContent>
//                 <DialogActions>
//                     <Button onClick={() => {
//                         setOpen(false)
//                     }}>Cancel</Button>
//                     <Button onClick={handleAddKey}>Add</Button>
//                 </DialogActions>
//             </Dialog>
//         </div>
//     );
// }


function KeysInternal() {
    let {userCreds} = useContext(AuthContext);
    let alert = useAlert();
    let [state, setState] = useState<State>({keys: undefined});
    const [selectionModel, setSelectionModel] = useState<GridSelectionModel>([]);
    useEffect(() => {
        if (state.keys === undefined)
            getTokens(userCreds).then((tokens) => {
                tokens ? setState({
                    keys: tokens
                }) : setState({keys: []})
            })
            .catch(error => {
                alert.showDanger(error.name, error.message);
            });
    })
    if (state.keys === undefined) {
        return (<><p>Loading...</p></>);
    }
    const columns: GridColDef[] = [
        {field: 'id', headerName: 'Key name', width: 250},
    ];

    console.log(state.keys)

    let rows: GridRowsProp = state.keys.map((key) => {
        return {
            id: key
        }
    });

    return (<DataGrid
        checkboxSelection
        disableSelectionOnClick
        autoHeight columns={columns}
        components={{}}
        componentsProps={{
            toolbar: {
                selectionModel,
                credentials: userCreds,
                update: () => setState({keys: undefined})
            }
        }}
        selectionModel={selectionModel}
        onSelectionModelChange={(newModel) => {
            setSelectionModel(newModel);
        }}
        rows={rows}
    />)
}

export function Keys() {
    return (<><KeysInternal/></>)
}