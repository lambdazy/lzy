import React from "react";
import {Button, Container, Form} from "react-bootstrap";
import {Redirect} from "react-router-dom";
import {getSession, login, Providers, useAuth} from "../logic/Auth";
import {useAlert} from "./ErrorAlert";
import {LzyLogo} from "./LzyLogo";

export interface LoginFormStateInterface {
    userId: string | null;
    userIdSet: boolean;
}

export interface LoginFormPropsInterface {
    onUserIdSet: (provider: Providers) => void;
}

export class LoginForm extends React.Component<LoginFormPropsInterface,
    LoginFormStateInterface> {
    constructor(props: LoginFormPropsInterface) {
        super(props);
        this.state = {userId: null, userIdSet: false};
    }

    handleSubmit = (provider: Providers): void => {
        this.setState({userIdSet: true});
        this.props.onUserIdSet(provider);
    };

    render() {
        if (!this.state.userIdSet) {
            return (
                <div>
                    <div className="App">
                        <div className="App__Svg">
                            <LzyLogo width="322" height="193"/>
                        </div>
                    </div>
                    <Container className="loginForm" fluid>
                        <Form>
                            <Button
                                variant="primary"
                                type="submit"
                                onClick={() => {
                                    this.handleSubmit(Providers.GITHUB)
                                }}
                            >
                                Login with github
                            </Button>
                        </Form>
                    </Container>
                </div>
            );
        }
        return <Redirect to="/"/>;
    }
}

export const LoginFormFC: React.FC<{}> = () => {
    let auth = useAuth();
    let alert = useAlert();
    return (
        <LoginForm
            onUserIdSet={(provider: Providers) => {
                auth.signout(() => {
                    getSession().then((res) => {
                        login(provider, res).then(
                            (res) => {
                                window.location.assign(res);
                            }
                        )
                            .catch((err) => {
                                alert.show(err.message, "Some error while logging in", undefined, "danger");
                            })
                    })
                });
            }}
        />
    );
};
