import React, {useContext} from "react";
import {Container} from "react-bootstrap";
import axios from "axios";
import {AuthContext} from "../logic/Auth";

import CRUDTable, {CreateForm, DeleteForm, Field, Fields,} from "react-crud-table";

import {useAlert} from "./ErrorAlert";
import {Permissions, PermittedComponent} from "../logic/Permissions";

export const UserTableFC: React.FC<{ host: string }> = ({host}) => {
    let {userCreds} = useContext(AuthContext);
    if (userCreds != null) {
        return <UsersTable host={host} userCredentials={userCreds}/>;
    }
    return <div/>;
};

export interface UsersTablePropsInterface {
    host: string;
    userCredentials: {
        userId: string,
        sessionId: string
    };
}

export function UsersTable(props: UsersTablePropsInterface) {
    let alert = useAlert();

    const fetchItems = () => {
        return axios
            .post(props.host + "/users/list", {credentials: props.userCredentials})
            .then((res) => {
                return res.data.users;
            })
            .catch((res) => {
                alert.showDanger("Error occurred while fetching users", res.message);
            });
    };

    const create = (userId: string) => {
        return axios
            .post(props.host + "/users/create", {
                creatorCredentials: props.userCredentials,
                user: {userId},
            })
            .catch((res) => {
                console.log(res);
                alert.showDanger("An error occurred while creating user", res.message);
            });
    };

    const deleteUser = (userId: string) => {
        return axios
            .post(props.host + "/users/delete", {
                deleterCredentials: props.userCredentials,
                userId,
            })
            .catch((res) => {
                console.log(res);
                alert.showDanger("An error occurred while deleting user", res.message);
            });
    };

    return (
        <div>
            <PermittedComponent permission={Permissions.USERS_LIST}>
                <Container className="p-3">
                    <CRUDTable fetchItems={() => fetchItems()}>
                        <Fields>
                            <Field name="userId" label="UserId"/>
                            <Field
                                name="tokens"
                                label="Tokens"
                                hideFromTable
                                hideInCreateForm
                            />
                        </Fields>
                        <PermittedComponent permission={Permissions.USERS_CREATE}>
                            <CreateForm
                                title="User Creation"
                                message="Create a new user"
                                trigger="Create User"
                                onSubmit={(task: { userId: string }) => create(task.userId)}
                                submitText="Create"
                            />
                        </PermittedComponent>
                        <PermittedComponent permission={Permissions.USERS_LIST}>
                            <DeleteForm
                                title="User Delete Process"
                                message="Are you sure you want to delete this user?"
                                trigger="Delete"
                                onSubmit={(task: { userId: string }) => deleteUser(task.userId)}
                                submitText="Delete"
                            />
                        </PermittedComponent>
                    </CRUDTable>
                </Container>
            </PermittedComponent>
        </div>
    );
}
