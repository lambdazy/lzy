import React from "react";
import "./App.css";
import {MainRouter} from "./widgets/MainRouter";
import {AuthProvider} from "./logic/Auth";
import {ProvideAlert} from "./widgets/ErrorAlert";
import Cookies from "universal-cookie/es6";

export const cookies = new Cookies();

function App() {
    return (
        <div>
            <AuthProvider>
                <ProvideAlert>
                    <MainRouter/>
                </ProvideAlert>
            </AuthProvider>
        </div>
    );
}

export default App;
