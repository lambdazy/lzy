import ast
import os
import signal
import subprocess
import sys
import time
import traceback
from typing import Any, Dict, Optional

from ipykernel.ipkernel import IPythonKernel

from lzy.api.v1 import op
from lzy.api.v1.env import LzyRemoteEnv, LzyRemoteWorkflow


EXCLUDED_SERVICE_VAR_NAMES = {
    'get_ipython', 'exit', 'quit',
    '__import__', '__spec__', '__name__', '__builtin__', '__builtins__', '__doc__', '__package__', '__loader__',
    '_i', '_ii', '_iii', '_ih', 'In',
    '_', '__', '___', '_oh', 'Out',
    '_dh'
}

DEBUG_OPA = True


def clear_namespace(ns: Dict[str, Any]) -> Dict[str, Any]:
    for var in EXCLUDED_SERVICE_VAR_NAMES:
        if var in ns:
            ns.pop(var)
    return ns


#
# jupyter console --existing kernel-XXX.json
#

class JupyterKernel(IPythonKernel):
    implementation = 'IdeKernel'
    implementation_version = '1.0'
    # language_info = {'mimetype': 'text/plain'}
    banner = 'JupyterKernel - as useful as a parrot'

    def __init__(self, **kwargs):
        super(JupyterKernel, self).__init__(**kwargs)
        self.log.error(f'JupyterKernel.__init__(), cwd={os.getcwd()}')

        self._user = os.getenv('LZY_USER', 'test-user')
        self._user_token = os.getenv('LZY_USER_TOKEN', 'xxx')
        self._mount = os.getenv('LZY_MOUNT', '/tmp/lzy')
        self._workflow_name = os.getenv('LZY_WORKFLOW', 'workflow-xxx')

        self._workflow: Optional[LzyRemoteWorkflow] = None
        self._state: Dict[str, Any] = {}

        self._terminal: Optional[subprocess.Popen] = None

    def start(self):
        super().start()

        self._check_java_version()
        self.log.error(f'Start BashApi at {self._mount}, and logs at /tmp/ide-kernel-{str(os.getpid())}, cwd={os.getcwd()}')
        try:
            self._terminal = subprocess.Popen(
                executable='java',
                args=[
                    '-Djava.library.path=/usr/local/lib',
                    '-Djava.util.concurrent.ForkJoinPool.common.parallelism=32',
                    '-Dlog4j.configurationFile=../servant/src/main/resources/servant_cmd/log4j2.yaml',
                    '-cp', '../servant/target/servant.jar',
                    'ai.lzy.servant.BashApi',
                    '-z', 'localhost:7777',
                    '-w', 'blah-blah-blah://localhost:8999',
                    '-m', self._mount,
                    '-h', 'localhost', '-p', '9990', '-q', '9991',
                    'terminal',
                    '-d',
                    '-u', self._user,
                    '-t', self._user_token,
                    '>/tmp/ide-kernel-' + str(os.getpid()),
                    '2>&1'
                ])

            while not os.path.exists(self._mount + '/sbin/terminal'):
                retcode = self._terminal.poll()
                if retcode is not None:
                    self.log.error("Terminal dead")
                    self.do_shutdown(False)
                    return
                self.log.info('.')
                time.sleep(1)
        except Exception as e:
            self.log.error('Can''t start terminal', e)
            raise e

        self.log.error(f'Init Lzy workflow {self._workflow_name}...')
        self._workflow = LzyRemoteEnv(self._mount).workflow(
            name=self._workflow_name,
            # conda_yaml_path=Path(''),  # todo: don't propagate current (kernel) python env
            eager=False)
        self._workflow.__enter__()
        self.log.error('JupyterKernel started')

    def execute_request(self, stream, ident, parent):
        return super().execute_request(stream, ident, parent)

    # noinspection PyMethodOverriding
    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if code.startswith('!'):
            return super().do_execute(code[1:], silent, store_history, user_expressions, allow_stdin)

        self.execution_count += 1

        @op
        def opa(code: str, state: Dict[str, Any]) -> Dict[str, Any]:
            _code = str(code)
            _state = dict(state)

            if DEBUG_OPA:
                with open('/tmp/opa-in', 'a') as f:
                    f.write('\n------\nCODE: `' + _code + '`, state: ' + repr(_state) + '\n')

            def _exec(expr, state):
                m = ast.Module([expr], type_ignores=[])
                exec(compile(m, '<>', 'exec'), state)
                return None

            try:
                tree = ast.parse(_code)

                if isinstance(tree.body[-1], ast.Expr):
                    # We try to emulate vanilla python kernel here.
                    # So if last statement is `ast.Expr` we evaluate it and print result.

                    for expr in tree.body[:-1]:
                        _exec(expr, _state)

                    last_expr = ast.Expression(tree.body[-1].value)
                    ret = eval(compile(last_expr, '<>', 'eval'), _state)
                    print(ret)

                    if DEBUG_OPA:
                        with open('/tmp/opa-out-ret', 'a') as f:
                            f.write('\n------\nexpr: ' + ast.dump(tree.body[-1]) + ', ret: ' + str(ret))
                else:
                    exec(_code, _state)

                clear_namespace(_state)
            except Exception as e:
                if DEBUG_OPA:
                    with open('/tmp/opa-fail', 'a') as f:
                        print(f"ERROR: code=`{_code}`, state=" + repr(_state) + "\n", e, file=f)
                raise e

            if DEBUG_OPA:
                with open('/tmp/opa-out', 'a') as f:
                    f.write('\n------\nstate: ' + repr(_state))

            return _state

        reply_content: Dict[str, Any] = {}
        try:
            self._state = opa(code, self._state)
            assert self._workflow is not None, "workflow is None"
            self._workflow.run()

            reply_content['status'] = 'ok'
            reply_content['user_expressions'] = self.shell.user_expressions(user_expressions or {})

            # dh: DisplayHook = self.shell.displayhook
            # dh.start_displayhook()
            # dh.write_output_prompt()
            # dh.fill_exec_result(new_state['__lzy_result__'])
            # dh.finish_displayhook()
        except Exception as err:
            reply_content['status'] = 'error'
            reply_content['user_expressions'] = {}
            # noinspection PyProtectedMember
            reply_content.update({
                "traceback": traceback.format_exception(*sys.exc_info()) or [],
                "ename": str(type(err).__name__),
                "evalue": str(err),
            })

            self.log.error("Exception in execute request:\n%s", "\n".join(reply_content["traceback"]))

        reply_content['execution_count'] = self.execution_count
        reply_content['payload'] = []

        return reply_content

    def do_shutdown(self, restart):
        if self._terminal is not None:
            self._terminal.send_signal(signal.SIGTERM)
            self._terminal.wait(10)

        self.log.error('Shutdown Lzy workflow')
        if self._workflow:
            self._workflow.__exit__(*sys.exc_info())
            self._workflow = None
        return super().do_shutdown(restart)

    @staticmethod
    def _check_java_version():
        with subprocess.Popen(
                ["bash", "-c", "java -version 2>&1 | head -1 | cut -d' ' -f3 | tr -d '\"'"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE
        ) as process:
            out, err = process.communicate()

            if process.returncode != 0:
                raise RuntimeError(f"Process exited with code {process.returncode}\n STDERR: " + str(err))

            if not out.decode('ascii').startswith('17.'):
                raise RuntimeError("Java 17 required")


if __name__ == '__main__':
    # noinspection PyPackageRequirements
    from ipykernel.kernelapp import IPKernelApp
    argv = [
        '--debug',
        # '--user=test-user',
        # '--help-all'
    ]
    IPKernelApp.launch_instance(kernel_class=JupyterKernel, argv=argv)
