from unittest import TestCase

from lzy.api import op, LzyEnv
from lzy.model.file_slots import OutFileSlot
from lzy.model.channel import Channel
from lzy.servant.bash_servant import exec_bash, BashServant, BashExecutionException


class BashServantTest(TestCase):
    def test_run_bash(self):
        result = exec_bash('ls', '/dev/null')
        self.assertEqual(result, b'/dev/null\n')

        def foo_raises():
            out = exec_bash("exit 1")
            print(out)

        self.assertRaises(BashExecutionException, foo_raises)

        try:
            foo_raises()
        except BashExecutionException as e:
            self.assertEqual('Process exited with code 1', e.message)

    def test_touch(self):
        servant = BashServant()
        file_name = f'{servant.mount()}/some_file.txt'

        slot = OutFileSlot(file_name)
        channel = Channel('channel_name')
        servant.touch(slot, channel)

        result = exec_bash(f'ls {file_name}')
        self.assertEqual(result, bytes(file_name + '\n'))

    def test_run(self):
        @op
        def foo(a: int) -> int:
            return a + 1

        with LzyEnv() as env:
            b = foo(2)

        self.assertEqual(b, 3)


