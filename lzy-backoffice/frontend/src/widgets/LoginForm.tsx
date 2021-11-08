import React from 'react';
import { Button, Container, Form } from "react-bootstrap";
import { Redirect } from 'react-router-dom';

import Cookies from 'universal-cookie';
import { LzyLogo } from './LzyLogo';

const cookies = new Cookies()

export interface LoginFormStateInterface{
    userId: string | null,
    userIdSet: boolean
}

export interface LoginFormPropsInterface{
    onUserIdSet: any
}

export class LoginForm extends React.Component<
    LoginFormPropsInterface,
    LoginFormStateInterface
>{
    constructor(props: LoginFormPropsInterface){
        super(props);
        this.state = {userId: null, userIdSet: false}
    }

    handleSubmit = (event: any):void =>{
        if (this.state.userId != null){
            this.setState({userIdSet: true})
            this.props.onUserIdSet(this.state.userId)
        }
    }

    handleChange = (event: any):void =>{
        this.setState({userId: event.target.value})
    }

    render() {
        if (!this.state.userIdSet){
            return (
                <div>
                <LzyLogo />
                <Container className="loginForm" fluid>
                    <Form>
                        <Form.Group className="mb-3" controlId="loginFormUserId">
                            <Form.Label>Username</Form.Label>
                            <Form.Control type="text" placeholder="Enter username" onChange={this.handleChange} />
                        </Form.Group>
                        <Button variant="primary" type="submit" onClick={this.handleSubmit}>
                            Ok
                        </Button>
                    </Form>
                </Container>
                </div>
            )
        }
        return (
            <Redirect to="/" />
        )
    }
}

export const LoginFormFC : React.FC<{}> = () =>{
    return (
        <LoginForm onUserIdSet = {(s: string) => {
            cookies.set("userId", s);
            console.log("Signed in")
            window.location.href = '/';
        }}/>
    )
}

