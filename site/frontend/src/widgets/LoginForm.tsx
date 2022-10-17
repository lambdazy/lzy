import React from "react";
import {Button, Container, Form} from "react-bootstrap";
import {Redirect} from "react-router-dom";
import {getProviderLoginUrl, AuthType} from "../logic/Auth";
import {useAlert} from "./ErrorAlert";
import {LzyLogo} from "./LzyLogo";

export interface LoginFormStateInterface {
    userId: string | null;
    userIdSet: boolean;
}

export interface LoginFormPropsInterface {
    onUserIdSet: (authType: AuthType) => void;
}

export class LoginForm extends React.Component<LoginFormPropsInterface, LoginFormStateInterface> {
    constructor(props: LoginFormPropsInterface) {
        super(props);
        this.state = {userId: null, userIdSet: false};
    }

    handleSubmit = (authType: AuthType): void => {
        this.setState({userIdSet: true});
        this.props.onUserIdSet(authType);
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
                                    this.handleSubmit(AuthType.GITHUB)
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

export const LoginFormFC: React.FC = () => {
    let alert = useAlert();
    return (
        <LoginForm
            onUserIdSet={(authType: AuthType) => {
                getProviderLoginUrl(authType).then(
                    (res) => {
                        window.location.assign(res);
                    }
                ).catch((err) => {
                    alert.showDanger("Error while logging in", err.message);
                });
            }}
        />
    );
};
