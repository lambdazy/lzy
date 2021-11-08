import { Switch, Route, Redirect } from 'react-router-dom';
import { LoginFormFC } from './LoginForm';
import { UserTableFC } from './UsersTable';
import Cookies from 'universal-cookie';
import { BACKEND_HOST } from '../config';

const cookies = new Cookies()

export const MainRouter = () => (
  <main>
    <Switch>
      <Route exact path='/login' component={LoginFormFC}/>
      {cookies.get('userId') != null && <Route path='/users' render={(props: any) => <UserTableFC host={BACKEND_HOST}/>}/>}
      {cookies.get('userId') == null && <Redirect to='/login'/>}
      {cookies.get('userId') != null && <Redirect to='/users'/>}
    </Switch>
  </main>
)