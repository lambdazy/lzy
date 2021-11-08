import React from 'react';
import { Button, Container, Form } from "react-bootstrap";
import axios from 'axios';

export interface AddTokenFormStateInterface{
    tokenName: string | null,
    token: string | null
}

export interface AddTokenFormPropsInterface{
    userId: string | null,
    host: string
}

export class LoginForm extends React.Component<
    AddTokenFormPropsInterface,
    AddTokenFormStateInterface
>{
    constructor(props: AddTokenFormPropsInterface){
        super(props);
        this.state = {token: null, tokenName: null}
    }

    handleSubmit = (event: any):void =>{
        if (this.state.token != null && this.state.tokenName != null){
            axios.post(this.props.host + "/users/add_token", {})
        }
    }

    handleChangeTokenName = (event: any):void =>{
        this.setState({tokenName: event.target.value})
    }

    handleChangeToken = (event: any):void =>{
        this.setState({tokenName: event.target.value})
    }

    render() {
        return (
            <Container className="p-3">
                <Form>
                    <Form.Group className="mb-3" controlId="loginFormUserId">
                        <Form.Label>Token name</Form.Label>
                        <Form.Control type="text" placeholder="Enter token name" onChange={this.handleChangeTokenName} />
                    </Form.Group>
                    <Form.Group className="mb-3" controlId="loginFormUserId">
                        <Form.Label>Token</Form.Label>
                        <Form.Control type="text" placeholder="Enter token" onChange={this.handleChangeToken} />
                    </Form.Group>
                    <Button variant="primary" type="submit" onClick={this.handleSubmit}>
                        Ok
                    </Button>
                </Form>
            </Container>
        )
    }
}