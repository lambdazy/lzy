import {Container, Navbar} from "react-bootstrap";
import {useHistory} from "react-router-dom";
import {AuthContext} from "../logic/Auth";
import {LzyLogo} from "./LzyLogo";
import {useContext} from "react";

export const Header = () => {
    let {userCreds, signOut} = useContext(AuthContext);
    let history = useHistory();
    return (
        <div>
            <Navbar bg="light">
                <Container>
                    <Navbar.Brand href="/">
                        <LzyLogo width="100" height="50"/>
                    </Navbar.Brand>
                    <Nav className="me-auto">
                        <Nav.Link href="/keys">My keys</Nav.Link>
                        <Nav.Link href="/tasks">Tasks</Nav.Link>
                    </Nav>
                    <Navbar.Collapse role={"row"} className="justify-content-end">
                        <Navbar.Text>
                            Signed in as:{" "}
                            <Button
                                onClick={() => {
                                    const goToLoginPage = () => {
                                        history.push("/login");
                                    };
                                    if (userCreds === null) {
                                        goToLoginPage();
                                    } else {
                                        signOut(goToLoginPage);
                                    }
                                }}
                            >
                                {userCreds != null && (<>{" "} {userCreds.userId}</>)}
                                {userCreds == null && (<>{" "} {"Login"}</>)}
                            </Button>
                        </Navbar.Text>
                    </Navbar.Collapse>
                </Container>
            </Navbar>
        </div>
    );
};
