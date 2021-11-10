import { useContext, createContext, useState, HTMLAttributes } from "react";
import { cookies } from "../App";
import { Route, Redirect } from "react-router-dom";

export const Auth = {
  isAuthenticated: false,
  signin(userId: UserCredentials, cb: () => void) {
    Auth.isAuthenticated = true;
    cb();
  },
  signout(cb: () => void) {
    Auth.isAuthenticated = false;
    cb();
  },
};

export interface AuthContext {
  userCredentials: UserCredentials | null;
  signin: (user: UserCredentials, cb: () => void) => void;
  signout: (cb: () => void) => void;
}

export interface UserCredentials {
  userId: string;
}

const authContext = createContext<AuthContext>({
  userCredentials: null,
  signin: () => {},
  signout: () => {},
});

export function useAuth(): AuthContext {
  return useContext(authContext);
}

export function useProvideAuth() {
  let creds = null;
  if (cookies.get("userId") != null) creds = { userId: cookies.get("userId") };
  const [user, setUser] = useState<UserCredentials | null>(creds);

  const signin = (user: UserCredentials, cb: () => void) => {
    return Auth.signin(user, () => {
      cookies.set("userId", user.userId);
      setUser(user);
      cb();
    });
  };

  const signout = (cb: () => void) => {
    return Auth.signout(() => {
      setUser(null);
      cookies.remove("userId");
      cb();
    });
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
