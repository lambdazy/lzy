import React, {useContext, useState} from "react";
import {Button, Container, Form} from "react-bootstrap";
import axios from "axios";
import {AuthContext} from "../logic/Auth";
import {useAlert} from "./ErrorAlert";

export interface AddTokenFormStateInterface {
    tokenName: string | null;
    token: string | null;
    stateLabel: string | null;
}

export interface AddTokenFormPropsInterface {
    host: string;
}

export function AddToken(props: AddTokenFormPropsInterface) {
    let [state, setState] = useState<AddTokenFormStateInterface>({
        token: null,
        tokenName: null,
        stateLabel: null
    });
    let alert = useAlert();
    let {userCreds} = useContext(AuthContext);

    const handleSubmit = (): void => {
        if (state.token != null && state.tokenName != null) {
            axios
                .post(props.host + "/users/add_token", {
                    userCredentials: userCreds,
                    token: state.token,
                    tokenName: state.tokenName,
                })
                .catch((error) => {
                    alert.showDanger("Error while adding token", error.message);
                })
                .then(() => {
                    alert.showSuccess("Success", "Token " + state.tokenName + " added!")
                })
        }
    };

    const handleChangeTokenName = (event: any): void => {
        setState({tokenName: event.target.value, token: state.token, stateLabel: state.stateLabel});
    };

    const handleChangeToken = (event: any): void => {
        setState({token: event.target.value, tokenName: state.tokenName, stateLabel: state.stateLabel});
    };

    return (
        <div>
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
                            as="textarea"
                            rows={6}
                            placeholder="Enter token"
                            onChange={handleChangeToken}
                        />
                    </Form.Group>
                    <Form.Label>
                        {state.stateLabel}
                    </Form.Label>
                    <Button variant="primary" onClick={handleSubmit}>
                        Ok
                    </Button>
                </Form>
            </Container>
        </div>
    );
}
