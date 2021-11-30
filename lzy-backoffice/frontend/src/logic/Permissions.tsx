import axios from "axios";
import { useEffect, useState } from "react";
import { useAsync } from "react-async";
import { ReactElement } from "react-markdown/lib/react-markdown";
import { BACKEND_HOST } from "../config";
import { useAlert } from "../widgets/ErrorAlert";
import { useAuth, UserCredentials } from "./Auth";

export enum Permissions{
    BACKOFFICE_INTERNAL = "backoffice.internal.privateApi",
    USERS_CREATE = "backoffice.users.create",
    USERS_DELETE = "backoffice.users.delete",
    USERS_LIST = "backoffice.users.list"
}

export async function checkPermission(credentials: UserCredentials, permission: Permissions): Promise<boolean> {
    let res = await axios.post(BACKEND_HOST() + "/auth/check_permission", {
        credentials,
        permissionName: permission.toString()
    })
    return res.data.granted;
}


export function PermittedComponent(props: { children: ReactElement, permission: Permissions}){
    let [component, setComponent] = useState<ReactElement>((<></>));
    let auth = useAuth();
    let {data, error} = useAsync({promiseFn: auth.getCredentials})
    let alert = useAlert();
    useEffect(() => {
        if (error){
            alert.show(error.message, error.name, () => {}, "danger");
        }
        if (data != null && component == null)
            checkPermission(data, props.permission).then((res: boolean) => {
                if (res){
                    setComponent(props.children);
                }
                else {
                    alert.show("You do not have permission: " + props.permission.valueOf(), "Permission denied", () => {}, "danger");
                    setComponent(<div></div>)
                }
            })
    })
    return(<>{component}</>)
}