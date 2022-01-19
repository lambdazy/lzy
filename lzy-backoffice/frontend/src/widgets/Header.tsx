import { useAsync } from "react-async";
import { Button, Container, Nav, Navbar } from "react-bootstrap";
import { useHistory } from "react-router-dom";
import { useAuth } from "../logic/Auth";
import { ErrorAlert, useAlert } from "./ErrorAlert";
import { LzyLogo } from "./LzyLogo";

export const Header = () => {
  let auth = useAuth();
  let history = useHistory();
  let {data, error} = useAsync({promiseFn: auth.getCredentials});
  let alert = useAlert();
  if (error){
    alert.show(error.message, error.name, () => {}, "danger");
  }
  return (
    <div>
      <Navbar bg="light">
        <Container>
          <Navbar.Brand href="/">
            <LzyLogo width="100" height="50"></LzyLogo>
          </Navbar.Brand>
          <Nav className="me-auto">
            <Nav.Link href="/users">Users</Nav.Link>
            <Nav.Link href="/keys">My keys</Nav.Link>
            <Nav.Link href="/tasks">Tasks</Nav.Link>
            <Nav.Link href="/whiteboards">Whiteboards</Nav.Link>
          </Nav>
          <Navbar.Collapse className="justify-content-end">
              <Navbar.Text>
                Signed in as:{" "}
                <Button
                  onClick={() => {
                    auth.signout(() => {
                      history.push("/login");
                    });
                  }}
                >
                  {data != null && (<>
                    {" "}
                    {data.userId}
                    </>
                  )}
                  {data == null && (<>
                    {"Login"}
                    </>
                  )}
                </Button>
              </Navbar.Text>
          </Navbar.Collapse>
        </Container>
      </Navbar>

      <ErrorAlert />
    </div>
  );
};
