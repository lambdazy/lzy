import { Switch, Route, Redirect } from "react-router-dom";
import { LoginFormFC } from "./LoginForm";
import { UserTableFC } from "./UsersTable";
import Cookies from "universal-cookie";
import { BACKEND_HOST } from "../config";
import { PrivateRoute } from "../logic/Auth";
import { AddToken } from "./AddToken";
import { AuthUser } from "./AuthUser";

export const MainRouter = () => (
  <main>
    <Switch>
      <PrivateRoute path="/users" exact>
        <UserTableFC host={BACKEND_HOST} />
      </PrivateRoute>
      <Route exact path="/login" component={LoginFormFC} />
      <Route exact path="/login_user" component={AuthUser} />
      <PrivateRoute path="/add_token" exact>
        <AddToken host={BACKEND_HOST} />
      </PrivateRoute>
      <Redirect to="/users"/>
    </Switch>
  </main>
);
