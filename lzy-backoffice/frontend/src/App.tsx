import React from "react";
import "./App.css";
import { LoginForm } from "./widgets/LoginForm";
import { UsersTable } from "./widgets/UsersTable";
import Cookies from "universal-cookie";
import { MainRouter } from "./widgets/Router";
import { Header } from "./widgets/Header";
import { ProvideAuth } from "./logic/Auth";
import { ErrorAlert, ProvideAlert } from "./widgets/ErrorAlert";

export const cookies = new Cookies();

class Main extends React.Component<{}, {}> {
  render() {
    return (
      <div>
        <ProvideAuth>
          <ProvideAlert>
            <ErrorAlert />
            <MainRouter />
          </ProvideAlert>
        </ProvideAuth>
      </div>
    );
  }
}

function App() {
  return <Main />;
}

export default App;
