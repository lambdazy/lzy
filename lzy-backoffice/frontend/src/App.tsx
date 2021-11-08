import React from 'react';
import './App.css';
import {LoginForm} from './widgets/LoginForm';
import {UsersTable} from './widgets/UsersTable';
import Cookies from 'universal-cookie';
import { MainRouter } from './widgets/Router';
import { Header } from './widgets/Header';

const cookies = new Cookies()

const UserTableFC: React.FC<{host: string}> = ({host}) =>{
  let userId: string | null = cookies.get("userId")
  if (userId != null){
    return <UsersTable host={host} userCredentials={{userId: userId}} />
  }
  return <div />;
}

class Main extends React.Component<{},{}>{
  render(){
    return (
      <div>
        <MainRouter />
      </div>
    )
  }
}

function App() {
  return (
    <Main />
  );
}

export default App;