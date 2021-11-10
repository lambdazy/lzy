import React from "react";
import { Button, Container, Form } from "react-bootstrap";
import axios from "axios";
import { useState } from "react";
import { useAuth } from "../logic/Auth";
import { useAlert } from "./ErrorAlert";
import { Header } from "./Header";

export interface AddTokenFormStateInterface {
  tokenName: string | null;
  token: string | null;
}

export interface AddTokenFormPropsInterface {
  host: string;
}

export function AddToken(props: AddTokenFormPropsInterface) {
  let [state, setState] = useState<AddTokenFormStateInterface>({
    token: null,
    tokenName: null,
  });

  let auth = useAuth();
  let alert = useAlert();

  const handleSubmit = (event: any): void => {
    if (state.token != null && state.tokenName != null) {
      axios
        .post(props.host + "/users/add_token", {
          userCredentials: auth.userCredentials,
          token: state.token,
          tokenName: state.tokenName,
        })
        .catch((error) => {
          alert.show(error.message, "Some error while adding token", undefined);
        });
    }
  };

  const handleChangeTokenName = (event: any): void => {
    setState({ tokenName: event.target.value, token: state.token });
  };

  const handleChangeToken = (event: any): void => {
    setState({ token: event.target.value, tokenName: state.tokenName });
  };

  return (
    <div>
      <Header />
      <Container className="p-3">
        <Form>
          <Form.Group className="mb-3" controlId="loginFormUserId">
            <Form.Label>Token name</Form.Label>
            <Form.Control
              type="text"
              placeholder="Enter token name"
              onChange={handleChangeTokenName}
            />
          </Form.Group>
          <Form.Group className="mb-3" controlId="loginFormUserId">
            <Form.Label>Token</Form.Label>
            <Form.Control
              type="text"
              placeholder="Enter token"
              onChange={handleChangeToken}
            />
          </Form.Group>
          <Button variant="primary" onClick={handleSubmit}>
            Ok
          </Button>
        </Form>
      </Container>
    </div>
  );
}
