import unittest
import io
import tarfile
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from xflow.framework.container import ContainerConnection
from xflow.framework.node import Node
from tests.test_node import FakeEnv
from tests.test_scripts import DummyPipeline
from tests.project_imports import use_project


class DummyContainer:
    name = "created"


class ContainerConnectionTest(unittest.TestCase):
    def make_connection_capturing_uploads(self):
        client = MagicMock()
        sock = MagicMock()
        archives = {}

        def exec_create(container_id, cmd, **kwargs):
            output = b'1000' if 'id -u' in cmd else b'flase'
            sock.read.side_effect = [output, b'']
            return {'Id': 'command'}

        def put_archive(directory, stream):
            with tarfile.open(fileobj=io.BytesIO(stream.read())) as archive:
                for member in archive.getmembers():
                    remote = f'{directory}/{member.name}'
                    archives[remote] = (
                        member.isfile(),
                        archive.extractfile(member).read() if member.isfile() else b'',
                    )

        client.api.exec_create.side_effect = exec_create
        client.api.exec_start.return_value = sock
        client.api.exec_inspect.return_value = {'ExitCode': 0}
        client.containers.get.return_value.put_archive.side_effect = put_archive
        for patcher in [
            patch('xflow.framework.container.DockerClient', return_value=client),
            patch('xflow.framework.container.select', return_value=([sock], [], [])),
        ]:
            self.addCleanup(patcher.stop)
            patcher.start()
        return ContainerConnection('localhost', name='existing'), archives

    def test_internal_script_links_deliver_content_at_declared_paths(self):
        for target_is_link in [True, False]:
            with self.subTest(target_is_link=target_is_link), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir).joinpath('scripts')
                root.joinpath('common').mkdir(parents=True)
                real = root.joinpath('common', 'real.sh')
                real.write_text('echo linked content')
                linkpath = 'link.sh' if target_is_link else 'common/helper.sh'
                root.joinpath(linkpath).symlink_to(real)
                target = linkpath if target_is_link else 'build.sh'
                if not target_is_link:
                    root.joinpath(target).write_text('. common/helper.sh')


                dependencies = {} if target_is_link else {target: [linkpath]}
                root.joinpath('__init__.py').write_text(
                    'from xflow.framework.scripts import Scripts as BaseScripts\n'
                    'class Scripts(BaseScripts):\n'
                    f'    DEPS = {dependencies!r}\n'
                    "    def prepare_release(self, argstr='', envs=None):\n"
                    f'        return self.__exec({target!r}, argstr, envs)\n'
                )
                conn, archives = self.make_connection_capturing_uploads()
                node = Node('container', 'builder', '/base', conn)
                use_project(self, tmpdir)
                pipeline = DummyPipeline(tmpdir, FakeEnv(node), 'container', DummyPipeline.Options())
                pipeline.buildid = 42

                pipeline.stage1()

                self.assertEqual(archives[f'/base/DummyPipeline/42/scripts/{linkpath}'],
                                 (True, b'echo linked content'))
                expected = {f'/base/DummyPipeline/42/scripts/{target}',
                            f'/base/DummyPipeline/42/scripts/{linkpath}'}
                self.assertEqual(set(archives), expected)

    def test_requires_exactly_one_container_name_or_image(self):
        with self.assertRaisesRegex(ValueError, "only one"):
            ContainerConnection("127.0.0.1")

        with self.assertRaisesRegex(ValueError, "only one"):
            ContainerConnection("127.0.0.1", name="existing", image="busybox")

    def test_client_certificate_and_key_must_be_paired(self):
        with self.assertRaisesRegex(ValueError, "clientcert"):
            ContainerConnection("127.0.0.1", name="existing", clientcert="cert.pem")

        with self.assertRaisesRegex(ValueError, "clientcert"):
            ContainerConnection("127.0.0.1", name="existing", clientkey="key.pem")

    def test_connection_string_and_existed_flag(self):
        existing = ContainerConnection("127.0.0.1", name="existing")
        self.assertTrue(existing.existed)
        self.assertEqual(existing._connstr, "docker://127.0.0.1:2375:existing")

        created = ContainerConnection("127.0.0.1", user="root", image="busybox")
        self.assertFalse(created.existed)
        self.assertEqual(created._connstr, "docker://root@127.0.0.1:2375:busybox->...")

        created._container = DummyContainer()
        self.assertEqual(created._connstr, "docker://root@127.0.0.1:2375:busybox->created")

    def test_default_locale_environment_is_set(self):
        conn = ContainerConnection("127.0.0.1", name="existing", envs={"LANG": "C.UTF-8"})

        self.assertEqual(conn._envs["LANG"], "C.UTF-8")
        self.assertEqual(conn._envs["LANGUAGE"], "en_US.UTF-8")
