import { useHistory } from "react-router";
import { useAuth } from "../logic/Auth";
import qs from "qs";

export function AuthUser(){
    let auth = useAuth()
    let history = useHistory()
    let params = qs.parse(window.location.search, { ignoreQueryPrefix: true })
    console.log(params)
    if (params.userId != null && params.sessionId != null)
        auth.signin(
                 {userId: params.userId.toString(), sessionId: params.sessionId?.toString()}, () => {history.push("/")})
    return (<div></div>)
}