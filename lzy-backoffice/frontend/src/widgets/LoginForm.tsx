import React from "react";
import { Button, Container, Form } from "react-bootstrap";
import { Redirect, useHistory } from "react-router-dom";
import { getSession, login, Providers, useAuth } from "../logic/Auth";
import { useAlert } from "./ErrorAlert";
import { LzyLogo } from "./LzyLogo";

export interface LoginFormStateInterface {
  userId: string | null;
  userIdSet: boolean;
}

export interface LoginFormPropsInterface {
  onUserIdSet: (userId: string, provider: Providers) => void;
}

export class LoginForm extends React.Component<
  LoginFormPropsInterface,
  LoginFormStateInterface
> {
  constructor(props: LoginFormPropsInterface) {
    super(props);
    this.state = { userId: null, userIdSet: false };
  }

  handleSubmit = (provider: Providers): void => {
    if (this.state.userId != null) {
      this.setState({ userIdSet: true });
      this.props.onUserIdSet(this.state.userId, provider);
    }
  };

  handleChange = (event: any): void => {
    this.setState({ userId: event.target.value });
  };

  render() {
    if (!this.state.userIdSet) {
      return (
        <div>
          <div className="App">
            <div className="App__Svg">
              <LzyLogo width="322" height="193" />
            </div>
          </div>
          <Container className="loginForm" fluid>
            <Form>
              <Form.Group className="mb-3" controlId="loginFormUserId">
                <Form.Label>Username</Form.Label>
                <Form.Control
                  type="text"
                  placeholder="Enter username"
                  onChange={this.handleChange}
                />
              </Form.Group>
              <Button
                variant="primary"
                type="submit"
                onClick={() => {this.handleSubmit(Providers.GITHUB)}}
              >
                  Login with github
              </Button>
            </Form>
          </Container>
        </div>
      );
    }
    return <Redirect to="/" />;
  }
}

export const LoginFormFC: React.FC<{}> = () => {
  let auth = useAuth();
  let alert = useAlert();
  return (
    <LoginForm
      onUserIdSet={(s: string, provider: Providers) => {
        auth.signout(() => {
            getSession().then((res) => {
                login(provider, s, res).then(
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
