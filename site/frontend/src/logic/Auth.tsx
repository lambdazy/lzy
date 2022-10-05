import {createContext, useContext, useState} from "react";
import {cookies} from "../App";
import {Redirect, Route, RouteProps} from "react-router-dom";
import axios from "axios";
import {BACKEND_HOST} from "../config";

export enum AuthType {
    GITHUB = "github"
}

export async function getProviderLoginUrl(authType: AuthType): Promise<string> {
    if (authType !== AuthType.GITHUB) {
        throw new Error("Unknown auth type " + authType);
    }
    const res = await axios.get(
        BACKEND_HOST() +
        "/auth/login_url?" +
        "authType=" + authType + "&" +
        "siteSignInUrl=" + window.location.protocol + "//" + window.location.host + "/login_user",
        {headers: {"Access-Control-Allow-Origin": "*"}}
    );
    return res.data.url;
}

export interface AuthContextInterface {
    userCreds: UserCredentials | null;
    signIn: (user: UserCredentials, cb: () => void) => void;
    signOut: (cb: () => void) => void;
}

export interface UserCredentials {
    userId: string,
    sessionId: string
}

export const AuthContext = createContext<AuthContextInterface>({
    userCreds: null,
    signIn: () => {},
    signOut: () => {},
});

export function AuthProvider(props: { children: any }) {
    const userId = cookies.get("userId");
    const sessionId = cookies.get("sessionId");
    const [userCreds, setUserCreds] = useState<UserCredentials | null>(
        userId === undefined ? null : {userId, sessionId});

    const signIn = (userCreds: UserCredentials, cb: () => void) => {
        cookies.set("userId", userCreds.userId);
        cookies.set("sessionId", userCreds.sessionId);
        setUserCreds(userCreds);
        cb();
    };

    const signOut = (cb: () => void) => {
        cookies.remove("userId");
        cookies.remove("sessionId");
        setUserCreds(null);
        cb();
    };

    return (
        <AuthContext.Provider value={{userCreds, signIn, signOut}}>
            {props.children}
        </AuthContext.Provider>
    );
}

type PrivateRouteProps = RouteProps;

export function PrivateRoute(props: PrivateRouteProps) {
    let {userCreds} = useContext(AuthContext);

    return (
        <Route
            {...props}
            render={({location}) =>
                userCreds ? (
                    props.children
                ) : (
                    <Redirect
                        to={{
                            pathname: "/login",
                            state: {from: location},
                        }}
                    />
                )
            }
        />
    );
}
