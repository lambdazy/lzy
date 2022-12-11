import axios from "axios";
import {useContext, useEffect, useState} from "react";
import {ReactElement} from "react-markdown/lib/react-markdown";
import {BACKEND_HOST} from "../config";
import {useAlert} from "../widgets/ErrorAlert";
import {AuthContext, UserCredentials} from "./Auth";

export enum Permissions {
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


export function PermittedComponent(props: { children: ReactElement, permission: Permissions }) {
    let [component, setComponent] = useState<ReactElement>((<></>));
    let {userCreds} = useContext(AuthContext);

    let alert = useAlert();
    useEffect(() => {
        if (userCreds != null && component == null)
            checkPermission(userCreds, props.permission).then((res: boolean) => {
                if (res) {
                    setComponent(props.children);
                } else {
                    alert.showDanger("Permission denied", "You do not have permission: " + props.permission.valueOf());
                    setComponent(<div/>)
                }
            })
    })
    return (<>{component}</>)
}