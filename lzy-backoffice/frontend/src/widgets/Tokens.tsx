import { Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle, TextField } from "@mui/material";
import { DataGrid, GridColDef, GridRowsProp, GridSelectionModel } from "@mui/x-data-grid";
import axios from "axios";
import { stringify } from "querystring";
import { useEffect, useState } from "react";
import { BACKEND_HOST } from "../config";
import { useAuth, UserCredentials } from "../logic/Auth"
import { useAlert } from "./ErrorAlert";
import { Header } from "./Header";

interface Token{
    name: string;
}

async function getTokens(credentials: UserCredentials | undefined | null): Promise<Token[]> {
    if (!credentials)
        return [];
    return (await axios.post(
        BACKEND_HOST() + "/public_keys/list",
        {credentials: credentials}
    )).data.keyNames;
}

interface State{
    tokens: Token[] | undefined,
    credentials: UserCredentials | undefined | null
}

interface ToolbarProps {
    selectionModel: GridSelectionModel,
    credentials: UserCredentials,
    update: () => void
}

function Toolbar(props: ToolbarProps) {
    const { selectionModel, credentials, update } = props;
  
    const handleClick = () => {
      if (!selectionModel) {
        return;
      }
      selectionModel.forEach((param: string | number) => {
        axios.post(BACKEND_HOST() + "/public_keys/delete", {keyName: param, credentials})
        .then(() => update())
      });
    };
  
    const handleMouseDown = (event: { preventDefault: () => void; }) => {
      // Keep the focus in the cell
      event.preventDefault();
    };

    let [open, setOpen] = useState<boolean>(false);
    let [tokenData, setTokenData] = useState<{name: string, value: string}>({name: "", value: ""});

    const handleAddToken = () => {
        axios.post(BACKEND_HOST() + "/public_keys/add", {keyName: tokenData.name, publicKey: tokenData.value, userCredentials: credentials})
        .then(() => {setOpen(false); update()})
    }
  
    return (
      <div>
        <Button
          onClick={handleClick}
          onMouseDown={handleMouseDown}
          disabled={!selectionModel || selectionModel.length==0}
          color="primary"
        >
          Delete
        </Button>
        <Button onClick={() => {setOpen(true)}}>
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
            onChange={(event) => {setTokenData({name: event.target.value, value: tokenData.value})}}
          />
          <TextField
            margin="dense"
            id="value"
            label="Key value"
            fullWidth
            multiline
            variant="standard"
            onChange={(event) => {setTokenData({name: tokenData.name, value: event.target.value})}}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => {setOpen(false)}}>Cancel</Button>
          <Button onClick={handleAddToken}>Add</Button>
        </DialogActions>
      </Dialog>
      </div>
    );
}


function TokensInternal(props: {}){
    let auth = useAuth();
    let alert = useAlert();
    let [state, setState] = useState<State>({tokens: undefined, credentials: undefined});
    const [selectionModel, setSelectionModel] = useState<GridSelectionModel>([]);
    useEffect(() => {
        if (state.credentials === undefined)
        auth.getCredentials().then(credentials => setState({credentials, tokens: state.tokens}))
        .catch(error => {alert.show(error.message, error.name, undefined, "danger");});

        if (state.tokens === undefined)
            getTokens(state.credentials).then((tokens) => {tokens ? setState({credentials: state.credentials,tokens}) : setState({credentials: state.credentials, tokens: []})})
            .catch(error => {alert.show(error.message, error.name, undefined, "danger");});
    })
    if (state.credentials === undefined || state.tokens === undefined){
        return (<><p>Loading...</p></>);
    }
    const columns: GridColDef[] = [
        { field: 'id', headerName: 'Token name', width: 250 },
    ];

    console.log(state.tokens)

    let rows: GridRowsProp = state.tokens.map((token) => {return {
        id: token
    }});

    return (<DataGrid 
        checkboxSelection
        disableSelectionOnClick
        autoHeight columns={columns}
        components={{Toolbar: Toolbar}}
        componentsProps={{
            toolbar: {
                selectionModel,
                credentials: state.credentials,
                update: () => setState({credentials: state.credentials, tokens: undefined})
            }
        }}
        selectionModel={selectionModel}
        onSelectionModelChange={(newModel) => {
            setSelectionModel(newModel);
        }}
        rows={rows}></DataGrid>)
}

export function Tokens(props: {}){
    return (<><Header /><TokensInternal /></>)
}