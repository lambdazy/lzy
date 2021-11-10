import { Button, Container, Nav, Navbar } from "react-bootstrap";
import { useHistory } from "react-router-dom";
import { useAuth } from "../logic/Auth";
import { ErrorAlert } from "./ErrorAlert";
import { LzyLogo } from "./LzyLogo";

export const Header = () => {
  let auth = useAuth();
  let history = useHistory();
  return (
    <div>
      <Navbar bg="light">
        <Container>
          <Navbar.Brand>
            <LzyLogo width="100" height="50"></LzyLogo>
          </Navbar.Brand>
          <Nav className="me-auto">
            <Nav.Link href="/users">Users</Nav.Link>
            <Nav.Link href="/add_token">Add token</Nav.Link>
          </Nav>
          <Navbar.Collapse className="justify-content-end">
            {auth.userCredentials != null && (
              <Navbar.Text>
                Signed in as:{" "}
                <Button
                  onClick={() => {
                    auth.signout(() => {
                      history.push("/login");
                    });
                  }}
                >
                  {" "}
                  {auth.userCredentials.userId}
                </Button>
              </Navbar.Text>
            )}
          </Navbar.Collapse>
        </Container>
      </Navbar>
      <ErrorAlert />
    </div>
  );
};
