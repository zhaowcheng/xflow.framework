import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from xflow.framework.node import Node
from xflow.framework.pipeline import Pipeline
from xflow.framework.ssh import CommandResult


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
        self.uploaded_text = {}

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
        if cmd.startswith("mkdir -p "):
            self.existing.add(cmd.removeprefix("mkdir -p "))
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
        pipeline = DummyPipeline(tmpdir.name, FakeEnv(node), "local", DummyPipeline.Options())
        pipeline.buildid = 42
        return node, conn, pipeline

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

    def test_exec_script_uploads_missing_script_once(self):
        node, conn, pipeline = self.make_node_and_pipeline()
        with tempfile.TemporaryDirectory() as tmpdir:
            script = Path(tmpdir).joinpath("build.sh")
            script.write_text("#!/bin/sh\necho ok\n")

            node.exec_script(script, "arg1")
            node.exec_script(script, "arg2")

        remote_script = "/base/DummyPipeline/42/scripts/build.sh"
        self.assertEqual(conn.exec_calls[0][0], "mkdir -p /base/DummyPipeline/42/scripts")
        self.assertEqual(conn.exec_calls[1][0], f"chmod +x {remote_script}")
        self.assertEqual(conn.exec_calls[2][0], f"{remote_script} arg1")
        self.assertEqual(conn.exec_calls[3][0], f"{remote_script} arg2")
        self.assertEqual(len(conn.putfile_calls), 1)
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
