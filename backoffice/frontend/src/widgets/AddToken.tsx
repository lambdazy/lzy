import React, {useState} from "react";
import {Button, Container, Form} from "react-bootstrap";
import axios from "axios";
import {useAuth} from "../logic/Auth";
import {useAlert} from "./ErrorAlert";
import {Header} from "./Header";
import {useAsync} from "react-async";

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

    let auth = useAuth();
    let {data, error} = useAsync({promiseFn: auth.getCredentials})
    let alert = useAlert();
    if (error) {
        alert.show(error.message, error.name, () => {
        }, "danger");
    }

    const handleSubmit = (event: any): void => {
        if (state.token != null && state.tokenName != null) {
            axios
                .post(props.host + "/users/add_token", {
                    userCredentials: data,
                    token: state.token,
                    tokenName: state.tokenName,
                })
                .catch((error) => {
                    alert.show(error.message, "Some error while adding token", undefined, "danger");
                })
                .then(() => {
                    alert.show("Token " + state.tokenName + " added!", "Success", () => {
                    }, "success")
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
            <Header/>
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
