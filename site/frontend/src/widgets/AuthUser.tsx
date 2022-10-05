import {useContext} from "react";
import {useHistory} from "react-router";
import {AuthContext} from "../logic/Auth";
import qs from "qs";

export function AuthUser() {
    let auth = useContext(AuthContext)
    let history = useHistory()
    let params = qs.parse(window.location.search, {ignoreQueryPrefix: true})
    if (params.userId != null && params.sessionId != null) {
        console.log(params)
        auth.signIn(
        {userId: params.userId.toString(), sessionId: params.sessionId?.toString()},
        () => {history.push("/")}
        )
    }
    return (<div/>)
}