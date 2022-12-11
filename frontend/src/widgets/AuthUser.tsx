import {useContext} from "react";
import {useHistory} from "react-router";
import {AuthContext} from "../logic/Auth";
import {cookies} from "../App";

export function AuthUser() {
    let auth = useContext(AuthContext)
    let history = useHistory()
    const userId = cookies.get("userId");
    const sessionId = cookies.get("sessionId");
    if (userId != null && sessionId != null) {
        auth.signIn(
        {userId, sessionId},
        () => {history.push("/")}
        )
    }
    return (<div/>)
}