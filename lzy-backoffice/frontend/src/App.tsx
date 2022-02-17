import React from "react";
import "./App.css";
import {MainRouter} from "./widgets/Router";
import {ProvideAuth} from "./logic/Auth";
import {ProvideAlert} from "./widgets/ErrorAlert";
import Cookies from "universal-cookie/es6";

export const cookies = new Cookies();

class Main extends React.Component<{}, {}> {
    render() {
        return (
            <div>
                <ProvideAuth>
                    <ProvideAlert>
                        <MainRouter/>
                    </ProvideAlert>
                </ProvideAuth>
            </div>
        );
    }
}

function App() {
    return <Main/>;
}

export default App;
