import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from xflow.framework.container import ContainerConnection
from xflow.framework.errors import CommandError
from xflow.framework.ssh import SSHConnection


class ConnectionDirectoryTest(unittest.TestCase):
    def setUp(self):
        self.patches = ExitStack()
        self.addCleanup(self.patches.close)

    def make_ssh(self):
        client = MagicMock()
        channel = MagicMock()
        channel.recv.return_value = b''
        commands = []

        def exec_command(cmd, **kwargs):
            commands.append((cmd, kwargs['environment']))
            channel.recv_exit_status.return_value = int(cmd.endswith('false'))
            stdout = MagicMock(channel=channel)
            return MagicMock(), stdout, MagicMock()

        client.exec_command.side_effect = exec_command
        self.patches.enter_context(patch('xflow.framework.ssh.SSHClient', return_value=client))
        self.patches.enter_context(patch('xflow.framework.ssh.select', return_value=([channel], [], [])))
        return SSHConnection('localhost', 'builder', 'unused'), commands

    def make_container(self):
        client = MagicMock()
        sock = MagicMock()
        sock.read.return_value = b''
        commands = []

        def exec_create(container_id, cmd, **kwargs):
            commands.append((cmd, kwargs['environment']))
            client.api.exec_inspect.return_value = {
                'ExitCode': int(cmd.endswith('false"')),
            }
            return {'Id': 'command'}

        client.api.exec_create.side_effect = exec_create
        client.api.exec_start.return_value = sock
        self.patches.enter_context(patch('xflow.framework.container.DockerClient', return_value=client))
        self.patches.enter_context(patch('xflow.framework.container.select', return_value=([sock], [], [])))
        return ContainerConnection('localhost', name='existing'), commands

    def assert_sdk_directories(self, commands, expected):
        self.assertEqual([cmd for cmd, _ in commands], expected)
        for _, envs in commands:
            self.assertEqual(envs['A'], 'B')

    def test_nested_directories_restore_outer_and_initial_sdk_commands(self):
        for factory, expected in [
            (self.make_ssh, ['cd /inner && pwd', 'cd /outer && pwd', 'pwd']),
            (self.make_container, [
                'bash -c "cd /inner && pwd"',
                'bash -c "cd /outer && pwd"',
                'bash -c "pwd"',
            ]),
        ]:
            with self.subTest(adapter=factory.__name__):
                conn, commands = factory()
                with conn.dir('/outer'):
                    with conn.dir('/inner'):
                        self.assertEqual(conn.exec('pwd', {'A': 'B'}).rc, 0)
                    conn.exec('pwd', {'A': 'B'})
                conn.exec('pwd', {'A': 'B'})
                self.assert_sdk_directories(commands, expected)

    def test_failed_command_restores_nested_sdk_directories(self):
        for factory, expected in [
            (self.make_ssh, ['cd /inner && false', 'cd /outer && pwd', 'pwd']),
            (self.make_container, [
                'bash -c "cd /inner && false"',
                'bash -c "cd /outer && pwd"',
                'bash -c "pwd"',
            ]),
        ]:
            with self.subTest(adapter=factory.__name__):
                conn, commands = factory()
                with conn.dir('/outer'):
                    with self.assertRaises(CommandError):
                        with conn.dir('/inner'):
                            conn.exec('false', {'A': 'B'})
                    conn.exec('pwd', {'A': 'B'})
                conn.exec('pwd', {'A': 'B'})
                self.assert_sdk_directories(commands, expected)

    def test_context_exception_restores_nested_sdk_directories(self):
        for factory, expected in [
            (self.make_ssh, ['cd /outer && pwd', 'pwd']),
            (self.make_container, ['bash -c "cd /outer && pwd"', 'bash -c "pwd"']),
        ]:
            with self.subTest(adapter=factory.__name__):
                conn, commands = factory()
                with self.assertRaisesRegex(RuntimeError, 'outer failed'):
                    with conn.dir('/outer'):
                        with self.assertRaisesRegex(RuntimeError, 'inner failed'):
                            with conn.dir('/inner'):
                                raise RuntimeError('inner failed')
                        conn.exec('pwd', {'A': 'B'})
                        raise RuntimeError('outer failed')
                conn.exec('pwd', {'A': 'B'})
                self.assert_sdk_directories(commands, expected)
