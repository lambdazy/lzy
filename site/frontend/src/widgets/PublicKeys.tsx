import {Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle, TextField} from "@mui/material";
import {DataGrid, GridColDef, GridRowsProp, GridSelectionModel} from "@mui/x-data-grid";
import axios, {AxiosResponse} from "axios";
import {useContext, useEffect, useState} from "react";
import {BACKEND_HOST} from "../config";
import {AuthContext, UserCredentials} from "../logic/Auth"
import {useAlert} from "./ErrorAlert";
import {Redirect} from "react-router-dom";

interface Key {
    name: string;
    value: string;
}

interface Keys {
    entries: Key[]
}

async function getKeys(credentials: UserCredentials): Promise<AxiosResponse> {
    return await axios.post(
        BACKEND_HOST() + "/key/list",
        {credentials: credentials}
    );
}

interface ToolbarProps {
    selectionModel: GridSelectionModel,
    credentials: UserCredentials,
    addKey: (key: Key) => void,
    deleteKey: (keyName: string) => void,
}

function Toolbar(props: ToolbarProps) {
    let alert = useAlert();
    const {selectionModel, credentials, addKey, deleteKey} = props;

    const handleClickDelete = () => {
        if (!selectionModel) {
            return;
        }
        selectionModel.forEach((keyName: string | number) => {
            if (typeof keyName === "number") {
                return;
            }
            axios.post(BACKEND_HOST() + "/key/delete", {keyName: keyName, credentials})
                .then(() => deleteKey(keyName))
                .catch(error => alert.showDanger("Unable to delete key " + keyName, error.message))
        });
    };

    const handleMouseDown = (event: { preventDefault: () => void; }) => {
        // Keep the focus in the cell
        event.preventDefault();
    };

    let [open, setOpen] = useState<boolean>(false);
    let [keyData, setKeyData] = useState<Key>({name: "", value: ""});

    const handleAddKey = () => {
        axios.post(BACKEND_HOST() + "/key/add", {
            keyName: keyData.name,
            publicKey: keyData.value,
            userCredentials: credentials
        }).then(() => {
            setOpen(false);
            addKey(keyData)
        })
    }

    return (
        <div>
            <Button
                onClick={handleClickDelete}
                onMouseDown={handleMouseDown}
                disabled={!selectionModel || selectionModel.length === 0}
                color="primary"
            >
                Delete
            </Button>
            <Button onClick={() => {
                setOpen(true)
            }}>
                Add
            </Button>
            <Dialog open={open}>
                <DialogTitle>Add key</DialogTitle>
                <DialogContent>
                    <DialogContentText>
                        Add new public key to server.
                    </DialogContentText>
                    <TextField
                        autoFocus
                        margin="dense"
                        id="name"
                        label="Key name"
                        fullWidth
                        variant="standard"
                        onChange={(event) => {
                            setKeyData({name: event.target.value, value: keyData.value})
                        }}
                    />
                    <TextField
                        margin="dense"
                        id="value"
                        label="Key value"
                        fullWidth
                        multiline
                        variant="standard"
                        onChange={(event) => {
                            setKeyData({name: keyData.name, value: event.target.value})
                        }}
                    />
                </DialogContent>
                <DialogActions>
                    <Button onClick={() => {
                        setOpen(false)
                    }}>Cancel</Button>
                    <Button onClick={handleAddKey}>Add</Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export function PublicKeys() {
    let {userCreds} = useContext(AuthContext);
    let alert = useAlert();
    let [keys, setKeys] = useState<Keys>({entries: []});
    const [selectionModel, setSelectionModel] = useState<GridSelectionModel>([]);

    useEffect(() => {
        if (userCreds === null) {
            return;
        }
        getKeys(userCreds).then((res) => {
            setKeys({entries: res.data.keys});
        })
            .catch(error => {
                alert.showDanger(error.name, error.message);
            })
    }, [alert, userCreds]);

    if (userCreds === null) {
        return <Redirect to="/login"/>;
    }

    const columns: GridColDef[] = [
        {field: 'id', headerName: 'Key name', width: 250},
        {field: 'value', headerName: 'Key value', width: 250},
    ];

    let rows: GridRowsProp = [];
    if (keys.entries) {
        rows = keys.entries.map(({name, value}) => {
            return {id: name, value}
        });
    }

    return <DataGrid
        checkboxSelection
        disableSelectionOnClick
        autoHeight columns={columns}
        components={{Toolbar}}
        componentsProps={{
            toolbar: {
                selectionModel,
                credentials: userCreds,
                addKey: (key: Key) => {
                    if (keys) {
                        setKeys({entries: keys.entries.concat([key])})
                    }
                },
                deleteKey: (keyName: string) => {
                    if (keys) {
                        const idx = keys.entries.findIndex(key => key.name === keyName);
                        if (idx > -1) {
                            keys.entries.splice(idx, 1);
                            setKeys({entries: keys.entries});
                        }
                    }
                }
            }
        }}
        selectionModel={selectionModel}
        onSelectionModelChange={(newModel) => {
            setSelectionModel(newModel);
        }}
        rows={rows}
    />
}