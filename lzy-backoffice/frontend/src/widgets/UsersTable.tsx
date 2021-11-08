import React from 'react';
import { Button, Container, Form } from "react-bootstrap";
import axios from 'axios';

import CRUDTable,
{
  Fields,
  Field,
  CreateForm,
  UpdateForm,
  DeleteForm,
} from 'react-crud-table';

import Cookies from 'universal-cookie';

import { Header } from './Header';

const cookies = new Cookies()

export const UserTableFC: React.FC<{host: string}> = ({host}) =>{
  let userId: string | null = cookies.get("userId")
  if (userId != null){
    return <UsersTable host={host} userCredentials={{userId: userId}} />
  }
  return <div />;
}


export interface UsersTableStateInterface{
    users: any[]
}

export interface UsersTablePropsInterface{
    host: string,
    userCredentials: {
        userId: string
    }
}

export class UsersTable extends React.Component<
    UsersTablePropsInterface,
    UsersTableStateInterface
>{
    constructor(props: UsersTablePropsInterface){
        super(props);
        this.state = {users: []}
    }

    fetchItems(){
        return axios.post(this.props.host + "/users/list", {credentials: this.props.userCredentials})
        .then((res) => {
            this.setState({users: res.data.users})
            return res.data.users
        })
        .catch((res) => {console.log(res)})
    }

    create(user_id: string){
        return axios.post(this.props.host + "/users/create", {
            creatorCredentials: this.props.userCredentials,
            user: {userId: user_id}
        })
        .catch((res) => {console.log(res)})
    }

    delete(user_id: string){
        return axios.post(this.props.host + "/users/delete", {
            deleterCredentials: this.props.userCredentials,
            userId: user_id
        })
        .catch((res) => {console.log(res)})
    }

    render() {
        return (
            <div>
                <Header />
                <Container className="p-3">
                    <CRUDTable
                    fetchItems={(_: any) => this.fetchItems()}
                    >
                    <Fields>
                        <Field
                        name="userId"
                        label="UserId"
                        />
                        <Field 
                        name="tokens"
                        label="Tokens"
                        hideFromTable
                        hideInCreateForm
                        />
                    </Fields>
                    <CreateForm
                        title="User Creation"
                        message="Create a new user"
                        trigger="Create User"
                        onSubmit={(task: {userId: string}) => this.create(task.userId)}
                        submitText="Create"
                    />
                    <DeleteForm
                        title="User Delete Process"
                        message="Are you sure you want to delete this user?"
                        trigger="Delete"
                        onSubmit={((task: {userId: string}) => this.delete(task.userId))}
                        submitText="Delete"
                    />
                    </CRUDTable>
                </Container>
            </div>
        )
    }
}