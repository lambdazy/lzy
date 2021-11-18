import { useContext, createContext, useState, HTMLAttributes } from "react";
import { cookies } from "../App";
import { Route, Redirect } from "react-router-dom";
import axios from "axios";
import { BACKEND_HOST } from "../config";
import AsyncLock from "async-lock";
import { useAsync } from "react-async"
import { useAlert } from "../widgets/ErrorAlert";
import Async from "react-async"

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
  getCredentials: () => Promise<UserCredentials | null>;
  signin: (user: UserCredentials, cb: () => void) => void;
  signout: (cb: () => void) => void;
}

export interface UserCredentials {
  userId: string,
  sessionId: string
}

const authContext = createContext<AuthContext>({
  getCredentials: async () => {return null},
  signin: () => {},
  signout: () => {},
});

export function useAuth(): AuthContext {
  return useContext(authContext);
}

const lock = new AsyncLock();

export async function getSession(): Promise<string> {
    return await lock.acquire("sessionId", async () => {
      if (cookies.get("sessionId") == null){
        const res = await axios.post(BACKEND_HOST() + "/auth/generate_session");
        cookies.set("sessionId", res.data.sessionId);
      }
      return cookies.get("sessionId");
    });
}

export function useProvideAuth() : AuthContext {
  const [user, setUser] = useState<UserCredentials | null>(null);

  const getCredentials = async () => {
    if (user != null){
      return user;
    }
    const sessionId = await getSession();
    if (cookies.get("userId") != null) {
      setUser({sessionId, userId: cookies.get("userId")});
      return {sessionId, userId: cookies.get("userId")};
    }
    return null;
  }

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
    getCredentials,
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
  let {data, error} = useAsync({promiseFn: auth.getCredentials})
  let alert = useAlert();
  if (data === undefined){
    alert.show("Please wait", "Login", () => {}, "success");
    return ( <> </>)
  }
  if (error){
    alert.show(error.message, error.name, () => {}, "danger");
    return ( <> </>)
  }
  alert.close()
  return (
    <Async promiseFn={auth.getCredentials}>
      <Async.Fulfilled>
        {data => (<Route
          {...props.rest}
          render={({ location }) =>
            data ? (
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
        />)}
      </Async.Fulfilled>
    </Async>
  );
}
