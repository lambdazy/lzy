import {Redirect, Route, Switch} from "react-router-dom";
import {LoginFormFC} from "./LoginForm";
import {PrivateRoute} from "../logic/Auth";
import {AuthUser} from "./AuthUser";
import {Basics, Environment, Setup, Data, Whiteboards, Views, Integrations, Overview} from "./Docs";
import {Tasks} from "./Tasks";
import {PublicKeys} from "./PublicKeys";
import {Header} from "./Header";

export const MainRouter = () => (
    <main>
        <Header/>
        <Switch>
            <PrivateRoute path="/tasks" exact>
                <Tasks/>
            </PrivateRoute>
            <Route exact path="/docs/0-overview.md" component={Overview}/>
            <Route exact path="/docs/1-setup.md" component={Setup}/>
            <Route exact path="/docs/2-basics.md" component={Basics}/>
            <Route exact path="/docs/3-data.md" component={Data}/>
            <Route exact path="/docs/4-environment.md" component={Environment}/>
            <Route exact path="/docs/5-whiteboards.md" component={Whiteboards}/>
            <Route exact path="/docs/6-views.md" component={Views}/>
            <Route exact path="/docs/7-integrations.md" component={Integrations}/>
            <Route exact path="/login" component={LoginFormFC}/>
            <Route exact path="/login_user" component={AuthUser}/>
            <PrivateRoute path="/keys" exact>
                <PublicKeys/>
            </PrivateRoute>
            <Redirect to="/docs/0-overview.md"/>
        </Switch>
    </main>
);
