import { useContext, createContext, useState, HTMLAttributes } from "react";
import { cookies } from "../App";
import { Route, Redirect } from "react-router-dom";
import axios from "axios";
import { BACKEND_HOST } from "../config";

export enum Providers{
    GITHUB = "github"
}

export async function login(provider: Providers, sessionId: string): Promise<string>{
    console.log(provider)
    const res = await axios.post(
        BACKEND_HOST() + "/auth/login",
        { sessionId, provider: provider.toString(), redirectUrl: "http://" + window.location.host + "/login_user" }
    );
    return res.data.redirectUrl;
}

export interface AuthContext {
  userCredentials: UserCredentials | null;
  signin: (user: UserCredentials, cb: () => void) => void;
  signout: (cb: () => void) => void;
}

export interface UserCredentials {
  userId: string,
  sessionId: string
}

const authContext = createContext<AuthContext>({
  userCredentials: null,
  signin: () => {},
  signout: () => {},
});

export function useAuth(): AuthContext {
  return useContext(authContext);
}

export async function getSession(): Promise<string> {
    if (cookies.get("sessionId") == null){
        const res = await axios.post(BACKEND_HOST() + "/auth/generate_session");
        cookies.set("sessionId", res.data.sessionId);
        return res.data.sessionId;
    }
    return cookies.get("sessionId");
}

export function useProvideAuth() {
  let creds = null;
  getSession();
  if (cookies.get("userId") != null && cookies.get("sessionId") != null){
      creds = {userId: cookies.get("userId"), sessionId: cookies.get("sessionId")}
  }
  const [user, setUser] = useState<UserCredentials | null>(creds);

  const signin = (user: UserCredentials, cb: () => void) => {
      cookies.set("userId", user.userId);
      setUser(user);
      cb();
  };

  const signout = (cb: () => void) => {
      setUser(null);
      cookies.remove("userId");
      cookies.remove("sessionId");
      cb();
  };

  return {
    userCredentials: user,
    signin,
    signout,
  };
}

export function ProvideAuth(props: { children: any }) {
  const auth = useProvideAuth();
  return (
    <authContext.Provider value={auth}>{props.children}</authContext.Provider>
  );
}

export function PrivateRoute(props: { children: any; [x: string]: any }) {
  let auth = useAuth();
  return (
    <Route
      {...props.rest}
      render={({ location }) =>
        auth.userCredentials ? (
          props.children
        ) : (
          <Redirect
            to={{
              pathname: "/login",
              state: { from: location },
            }}
          />
        )
      }
    />
  );
}
