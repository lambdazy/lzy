import { Switch, Route, Redirect } from "react-router-dom";
import { LoginFormFC } from "./LoginForm";
import { UserTableFC } from "./UsersTable";
import Cookies from "universal-cookie";
import { BACKEND_HOST } from "../config";
import { PrivateRoute } from "../logic/Auth";
import { AddToken } from "./AddToken";

export const MainRouter = () => (
  <main>
    <Switch>
      <Route exact path="/login" component={LoginFormFC} />
      <PrivateRoute path="/users" exact>
        <UserTableFC host={BACKEND_HOST} />{" "}
      </PrivateRoute>
      <PrivateRoute path="/add_token" exact>
        <AddToken host={BACKEND_HOST} />{" "}
      </PrivateRoute>
      <Route exact path="/">
        <Redirect to="/users" />
      </Route>
      <Redirect to="/" />
    </Switch>
  </main>
);
