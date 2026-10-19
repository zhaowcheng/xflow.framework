import tempfile
import unittest
import shlex
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from xflow.framework.node import Node
from xflow.framework.pipeline import Pipeline
from xflow.framework.ssh import CommandResult
from xflow.framework.errors import CommandError
from tests.project_imports import use_project


class FakeConnection:
    existed = True

    def __init__(self):
        self.current_dir = ""
        self.dir_calls = []
        self.exec_calls = []
        self.getfile_calls = []
        self.putfile_calls = []
        self.remove_calls = []
        self.existing = set()
        self.exists_calls = []
        self.uploaded_text = {}
        self.failing_commands = set()

    @contextmanager
    def dir(self, path):
        self.dir_calls.append(path)
        previous = self.current_dir
        self.current_dir = str(path)
        try:
            yield
        finally:
            self.current_dir = previous

    def exec(self, cmd, envs=None):
        self.exec_calls.append((cmd, envs, self.current_dir))
        if cmd in self.failing_commands:
            raise CommandError(cmd)
        if cmd.startswith("mkdir -p "):
            self.existing.add(shlex.split(cmd)[-1])
        return CommandResult("ok", rc=0, cmd=cmd)

    def getfile(self, rfile, ldir):
        self.getfile_calls.append((rfile, ldir))

    def putfile(self, lfile, rdir):
        lfile = Path(lfile)
        rdir = PurePosixPath(rdir)
        rfile = rdir.joinpath(lfile.name)
        self.putfile_calls.append((lfile, rdir))
        self.existing.add(str(rfile))
        try:
            self.uploaded_text[str(rfile)] = lfile.read_text()
        except (FileNotFoundError, UnicodeDecodeError):
            pass

    def exists(self, path):
        self.exists_calls.append(str(path))
        return str(path) in self.existing

    def remove(self, force=False):
        self.remove_calls.append(force)


class DummyPipeline(Pipeline):
    def stage1(self):
        pass


class FakeEnv:
    def __init__(self, node):
        self.node = node

    def get_node(self, name):
        return self.node


class NodeTest(unittest.TestCase):
    def make_node_and_pipeline(self):
        conn = FakeConnection()
        node = Node("local", "builder", "/base", conn)
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        root = Path(tmpdir.name).joinpath('scripts')
        root.mkdir()
        root.joinpath('__init__.py').write_text(
            'from xflow.framework.scripts import Scripts as BaseScripts\n'
            'class Scripts(BaseScripts):\n    DEPS = {}\n'
        )
        use_project(self, tmpdir.name)
        pipeline = DummyPipeline(tmpdir.name, FakeEnv(node), "local", DummyPipeline.Options())
        pipeline.buildid = 42
        return node, conn, pipeline

    def test_nested_directories_restore_outer_and_initial_workdirs(self):
        node, conn, pipeline = self.make_node_and_pipeline()

        with node.dir('outer'):
            with node.dir('inner'):
                node.exec('pwd')
            node.exec('pwd')
        node.exec('pwd')

        self.assertEqual([call[2] for call in conn.exec_calls], [
            '/base/DummyPipeline/42/inner',
            '/base/DummyPipeline/42/outer',
            '/base/DummyPipeline/42',
        ])

    def test_failed_command_restores_nested_directories(self):
        node, conn, pipeline = self.make_node_and_pipeline()
        conn.failing_commands.add('false')

        with node.dir('/outer'):
            with self.assertRaises(CommandError):
                with node.dir('/inner'):
                    node.exec('false')
            node.exec('pwd')
        node.exec('pwd')

        self.assertEqual([call[2] for call in conn.exec_calls], [
            '/inner', '/outer', '/base/DummyPipeline/42',
        ])

    def test_context_exception_restores_nested_directories(self):
        node, conn, pipeline = self.make_node_and_pipeline()

        with self.assertRaisesRegex(RuntimeError, 'context failed'):
            with node.dir('/outer'):
                with self.assertRaisesRegex(RuntimeError, 'inner failed'):
                    with node.dir('/inner'):
                        raise RuntimeError('inner failed')
                node.exec('pwd')
                raise RuntimeError('context failed')
        node.exec('pwd')

        self.assertEqual([call[2] for call in conn.exec_calls], [
            '/outer', '/base/DummyPipeline/42',
        ])

    def test_cwd_uses_current_pipeline_and_dir_context(self):
        node, _, pipeline = self.make_node_and_pipeline()

        self.assertEqual(node.cwd, PurePosixPath("/base/DummyPipeline/42"))

        with node.dir("src"):
            self.assertEqual(node.cwd, PurePosixPath("/base/DummyPipeline/42/src"))

        with node.dir("/absolute"):
            self.assertEqual(node.cwd, PurePosixPath("/absolute"))

        self.assertEqual(node.cwd, PurePosixPath("/base/DummyPipeline/42"))
        self.assertIs(pipeline.node, node)

    def test_mkcwd_and_rmcwd_execute_against_pipeline_workdir(self):
        node, conn, pipeline = self.make_node_and_pipeline()

        node.mkcwd()
        node.rmcwd()

        self.assertEqual(
            conn.exec_calls,
            [
                ("mkdir -p /base/DummyPipeline/42", None, ""),
                ("rm -rf /base/DummyPipeline/42", None, ""),
            ],
        )
        self.assertIs(pipeline.node, node)

    def test_exec_runs_inside_current_workdir(self):
        node, conn, pipeline = self.make_node_and_pipeline()

        with node.dir("src"):
            result = node.exec("make test", envs={"A": "B"})

        self.assertEqual(result, "ok")
        self.assertEqual(conn.dir_calls, [PurePosixPath("/base/DummyPipeline/42/src")])
        self.assertEqual(conn.exec_calls, [("make test", {"A": "B"}, "/base/DummyPipeline/42/src")])
        self.assertIs(pipeline.node, node)

    def test_nixenv_wraps_command_and_clears_after_context(self):
        node, conn, pipeline = self.make_node_and_pipeline()

        with node.nixenv("/flake", system="x86_64-linux", name="dev", options="--impure"):
            node.exec("make")
        node.exec("pwd")

        self.assertEqual(
            conn.exec_calls[0][0],
            "nix develop /flake#devShells.x86_64-linux.dev --impure --log-format raw -c make",
        )
        self.assertEqual(conn.exec_calls[1][0], "pwd")
        self.assertIs(pipeline.node, node)


    def test_file_helpers_delegate_to_connection(self):
        node, conn, pipeline = self.make_node_and_pipeline()

        node.getfile("/remote/out.txt", "/local")
        node.putfile("/local/in.txt", "/remote")
        self.assertFalse(node.exists("/missing"))

        self.assertEqual(conn.getfile_calls, [("/remote/out.txt", "/local")])
        self.assertEqual(conn.putfile_calls[-1], (Path("/local/in.txt"), PurePosixPath("/remote")))
        self.assertIs(pipeline.node, node)

    def test_write_uploads_text_file_to_remote_directory(self):
        node, conn, pipeline = self.make_node_and_pipeline()

        node.write("hello", "/remote/message.txt")

        self.assertEqual(conn.uploaded_text["/remote/message.txt"], "hello")
        self.assertIs(pipeline.node, node)

    def test_git_clones_and_checks_out_revision(self):
        node, conn, pipeline = self.make_node_and_pipeline()

        node.git("https://example.invalid/repo.git", "v1.0", options="--depth 1")

        self.assertEqual(conn.exec_calls[0][0], "git clone --depth 1 https://example.invalid/repo.git repo")
        self.assertEqual(conn.dir_calls[1], PurePosixPath("/base/DummyPipeline/42/repo"))
        self.assertEqual(conn.exec_calls[1][0], "git checkout v1.0")
        self.assertIs(pipeline.node, node)
