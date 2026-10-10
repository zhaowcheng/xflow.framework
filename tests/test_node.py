import tempfile
import os
import shlex
import shutil
import subprocess
import unittest
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from xflow.framework.node import Node
from xflow.framework.pipeline import Pipeline
from xflow.framework.ssh import CommandResult
from xflow.framework.errors import CommandError


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
        self.fail_at = None
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
        if self.fail_at and cmd.startswith(self.fail_at):
            raise RuntimeError(self.fail_at)
        if cmd.startswith("mkdir -p "):
            self.existing.add(shlex.split(cmd)[-1])
        return CommandResult("ok", rc=0, cmd=cmd)

    def getfile(self, rfile, ldir):
        self.getfile_calls.append((rfile, ldir))

    def putfile(self, lfile, rdir):
        if self.fail_at == "upload":
            raise RuntimeError("upload")
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
        self.assertEqual(conn.exec_calls[3][0], f"chmod +x {remote_script}")
        self.assertEqual(conn.exec_calls[4][0], f"{remote_script} arg2")
        self.assertEqual(len(conn.putfile_calls), 1)
        self.assertIs(pipeline.node, node)

    def test_exec_script_delivers_recursive_dependencies_from_common_root(self):
        node, conn, pipeline = self.make_node_and_pipeline()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "lib").mkdir()
            (root / "build.sh").write_text("#!/bin/sh\nsource lib/common.sh\n")
            (root / "lib/common.sh").write_text("source shared.sh\n")
            (root / "shared.sh").write_text("echo shared\n")

            node.exec_script(root / "build.sh", "arg", envs={"A": "B"})

        self.assertEqual(set(conn.uploaded_text), {
            "/base/DummyPipeline/42/scripts/build.sh",
            "/base/DummyPipeline/42/scripts/lib/common.sh",
            "/base/DummyPipeline/42/scripts/shared.sh",
        })
        self.assertEqual(conn.exec_calls[-1], (
            "/base/DummyPipeline/42/scripts/build.sh arg", {"A": "B"},
            "/base/DummyPipeline/42/scripts",
        ))
        self.assertEqual(node.cwd, PurePosixPath("/base/DummyPipeline/42"))
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


class LocalShellConnection(FakeConnection):
    def exec(self, cmd, envs=None):
        super().exec(cmd, envs)
        process = subprocess.run(
            cmd, shell=True, cwd=self.current_dir or None,
            env={**os.environ, **(envs or {})}, capture_output=True, text=True,
        )
        if process.returncode:
            raise CommandError(process.stderr)
        return CommandResult(process.stdout, rc=0, cmd=cmd)

    def exists(self, path):
        self.exists_calls.append(str(path))
        return Path(path).exists()

    def putfile(self, lfile, rdir):
        super().putfile(lfile, rdir)
        shutil.copyfile(lfile, Path(rdir) / Path(lfile).name)


class ScriptDependenciesTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="script dependencies ")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name) / "local scripts"
        self.root.mkdir()
        self.remote = Path(temp.name) / "remote scripts"

    def write_script(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def make_node(self, real_shell=False):
        conn = LocalShellConnection() if real_shell else FakeConnection()
        node = Node("local", "builder", str(self.remote), conn)
        pipeline = DummyPipeline(str(self.root), FakeEnv(node), "local", DummyPipeline.Options())
        pipeline.buildid = 42
        return node, conn, pipeline

    def uploaded(self, conn):
        prefix = str(self.remote / "DummyPipeline/42/scripts") + "/"
        return {path.removeprefix(prefix) for path in conn.uploaded_text}

    def assert_no_remote_operations(self, conn):
        self.assertEqual(conn.exists_calls, [])
        self.assertEqual(conn.exec_calls, [])
        self.assertEqual(conn.putfile_calls, [])
        self.assertEqual(conn.dir_calls, [])

    def test_missing_dependency_and_cycles_fail_before_remote_operations(self):
        for text, dependency, expected in [
            ("source missing.sh", None, "missing.sh"),
            ("source build.sh", None, "build.sh -> build.sh"),
            ("source common.sh", "source build.sh", "build.sh -> common.sh -> build.sh"),
            ("source ../outside.sh", None, "parent"),
            ("source ./build.sh", None, "build.sh -> build.sh"),
        ]:
            with self.subTest(text=text):
                node, conn, pipeline = self.make_node()
                script = self.write_script("build.sh", text + "\n")
                if dependency:
                    self.write_script("common.sh", dependency + "\n")
                with self.assertRaisesRegex(ValueError, expected):
                    node.exec_script(script)
                self.assert_no_remote_operations(conn)

    def test_reparse_deduplicates_and_repairs_existing_target(self):
        node, conn, pipeline = self.make_node()
        script = self.write_script("build.sh", "source a/common.sh\nsource b/common.sh\n")
        self.write_script("a/common.sh", "source shared.sh\n")
        self.write_script("b/common.sh", "source shared.sh\n")
        self.write_script("shared.sh", "echo shared\n")
        node.exec_script(script)
        self.assertEqual(self.uploaded(conn), {"build.sh", "a/common.sh", "b/common.sh", "shared.sh"})
        self.assertEqual(len(conn.putfile_calls), 4)
        node.exec_script(script)
        self.assertEqual(len(conn.putfile_calls), 4)
        conn.existing.remove(str(node.scriptdir / "shared.sh"))
        node.exec_script(script)
        self.assertEqual(len(conn.putfile_calls), 5)
        self.write_script("new.sh", "echo new\n")
        script.write_text(script.read_text() + "source new.sh\n")
        node.exec_script(script)
        self.assertEqual(len(conn.putfile_calls), 6)
        self.assertIn("new.sh", self.uploaded(conn))
        script.write_text("source missing.sh\n")
        conn.exec_calls.clear()
        conn.exists_calls.clear()
        conn.putfile_calls.clear()
        conn.dir_calls.clear()
        with self.assertRaisesRegex(ValueError, "missing.sh"):
            node.exec_script(script)
        self.assert_no_remote_operations(conn)

    def test_delivery_failures_stop_execution_and_permission_retry_succeeds(self):
        for failure in ["mkdir -p", "upload", "chmod +x"]:
            with self.subTest(failure=failure):
                node, conn, pipeline = self.make_node()
                script = self.write_script("build.sh", "#!/bin/sh\necho ok\n")
                conn.fail_at = failure
                with self.assertRaisesRegex(RuntimeError, failure.replace("+", r"\+")):
                    node.exec_script(script)
                self.assertTrue(all(cmd.startswith(("mkdir -p", "chmod +x")) for cmd, _, _ in conn.exec_calls))
                conn.fail_at = None
                result = node.exec_script(script)
                self.assertEqual(result, "ok")
                self.assertTrue(conn.exec_calls[-2][0].startswith("chmod +x"))

    def test_five_reference_forms_quotes_continuations_and_shell_execution(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '''#!/bin/bash
source "lib/source file.sh" source-arg
. ./lib/dot.sh
bash -eu -- "lib/bash file.sh" "arg with spaces"
sh -e lib/sh.sh sh-arg
./lib/direct.sh direct-arg
source \\
    "lib/source file.sh" repeated
''')
        self.write_script("lib/source file.sh", 'printf "source:%s\\n" "$1"\n')
        self.write_script("lib/dot.sh", 'echo dot\n')
        self.write_script("lib/bash file.sh", '#!/bin/bash\nsource shared.sh\nprintf "bash:%s\\n" "$1"\n')
        self.write_script("lib/sh.sh", 'printf "sh:%s\\n" "$1"\n')
        self.write_script("lib/direct.sh", '#!/bin/sh\nprintf "direct:%s\\n" "$1"\n')
        self.write_script("shared.sh", 'echo shared\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).splitlines(), [
            "source:source-arg", "dot", "shared", "bash:arg with spaces",
            "sh:sh-arg", "direct:direct-arg", "source:repeated",
        ])
        self.assertEqual(self.uploaded(conn), {
            "build.sh", "lib/source file.sh", "lib/dot.sh", "lib/bash file.sh",
            "lib/sh.sh", "lib/direct.sh", "shared.sh",
        })
        self.assertEqual(len(conn.putfile_calls), 7)

    def test_comments_strings_and_heredocs_are_not_dependencies(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '''#!/bin/bash
# source missing-comment.sh
echo 'source missing-string.sh; . missing-dot.sh'
echo "bash missing-bash.sh; sh missing-sh.sh; ./missing-direct.sh"
cat <<'TEXT' > /dev/null
source missing-heredoc.sh
. missing-heredoc.sh
bash missing-heredoc.sh
sh missing-heredoc.sh
./missing-heredoc.sh
TEXT
cat <<-TEXT > /dev/null
\tsource missing-tab.sh
\tTEXT
source present.sh; . present.sh # source missing-tail.sh
bash -c 'echo inline'
''')
        self.write_script("present.sh", 'echo present\n')
        node.exec_script(script)
        self.assertEqual(self.uploaded(conn), {"build.sh", "present.sh"})

    def test_conditional_references_are_checked_before_upload(self):
        node, conn, pipeline = self.make_node()
        script = self.write_script("build.sh", "#!/bin/sh\nif false; then . missing.sh; fi\n")
        with self.assertRaisesRegex(ValueError, r"build.sh:2:.*missing.sh"):
            node.exec_script(script)
        self.assert_no_remote_operations(conn)

    def test_constant_assignments_concatenation_and_source_scope(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '''#!/bin/bash
LIB='lib files'
readonly NAME=common.sh
export DEP="$LIB/$NAME"
source "$DEP"
source "${NEXT}"
''')
        self.write_script("lib files/common.sh", 'NEXT=next.sh\necho common\n')
        self.write_script("next.sh", 'echo next\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).splitlines(), ["common", "next"])
        self.assertEqual(self.uploaded(conn), {"build.sh", "lib files/common.sh", "next.sh"})

    def test_dynamic_paths_fail_with_location_without_executing_substitutions(self):
        marker = self.root / "side-effect"
        for expression in [
            '"$UNKNOWN"', '"${UNKNOWN}"', '"${DEP:-fallback.sh}"',
            '"$(touch ' + shlex.quote(str(marker)) + ')"',
            '"`touch ' + shlex.quote(str(marker)) + '`"',
            '"$DEP"', './$UNKNOWN.sh', '"$UNKNOWN/run.sh"',
        ]:
            with self.subTest(expression=expression):
                node, conn, pipeline = self.make_node()
                script = self.write_script("build.sh", '#!/bin/bash\nDEP=$(echo run.sh)\nsource ' + expression + '\n')
                with self.assertRaisesRegex(ValueError, r"build.sh:3:.*unresolved"):
                    node.exec_script(script)
                self.assert_no_remote_operations(conn)
                self.assertFalse(marker.exists())

    def test_conditional_and_runtime_variable_assignments_are_not_constants(self):
        for text in [
            'if false; then DEP=other.sh; fi\nsource "$DEP"\n',
            'read DEP\nsource "$DEP"\n',
            'for DEP in other.sh; do source "$DEP"; done\n',
        ]:
            with self.subTest(text=text):
                node, conn, pipeline = self.make_node()
                script = self.write_script("build.sh", 'DEP=present.sh\n' + text)
                self.write_script("present.sh", 'echo present\n')
                with self.assertRaisesRegex(ValueError, "unresolved"):
                    node.exec_script(script)
                self.assert_no_remote_operations(conn)

    def test_direct_remote_references_and_shell_modes_do_not_read_local_paths(self):
        node, conn, pipeline = self.make_node()
        script = self.write_script("build.sh", '''#!/bin/bash
source /remote/source.sh
. ~/dot.sh
bash -e /remote/bash.sh
sh ~/sh.sh
/remote/direct.sh
~/direct.sh
bash -c 'source missing-inline.sh'
sh -s missing-argument.sh
source './dollar$literal.sh'
''')
        self.write_script("dollar$literal.sh", 'echo literal\n')
        node.exec_script(script)
        self.assertEqual(self.uploaded(conn), {"build.sh", "dollar$literal.sh"})

    def test_script_directory_and_common_root_have_distinct_shell_semantics(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '''#!/bin/bash
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
source "$SCRIPT_DIR/deep/common.sh"
bash "$SCRIPT_DIR/deep/child.sh"
''')
        self.write_script("deep/common.sh", '''HERE=$(dirname "${BASH_SOURCE[0]}")
source "$HERE/local.sh"
source shared.sh
''')
        self.write_script("deep/child.sh", '''#!/bin/sh
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/local.sh"
. shared.sh
''')
        self.write_script("deep/local.sh", 'echo self-directory\n')
        self.write_script("shared.sh", 'echo common-root\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).splitlines(), [
            "self-directory", "common-root", "self-directory", "common-root",
        ])
        self.assertEqual(self.uploaded(conn), {
            "build.sh", "deep/common.sh", "deep/child.sh", "deep/local.sh", "shared.sh",
        })
        for local, remote_dir in conn.putfile_calls:
            self.assertEqual(conn.uploaded_text[str(remote_dir / local.name)], local.read_text())

    def test_directory_expression_errors_and_parent_paths_stop_before_upload(self):
        for text, expected in [
            ('HERE="$(dirname "${BASH_SOURCE[0]}")"\nsource "$HERE/missing.sh"', "missing.sh"),
            ('HERE="$(dirname "$0")"\nsource "$HERE/../outside.sh"', "parent"),
            ('HERE="$(dirname "$0")"\nsource "$HERE/build.sh"', "build.sh -> build.sh"),
            ('HERE="$(cd "$(dirname "$0")" && echo unsafe && pwd)"\nsource "$HERE/common.sh"', "unresolved"),
        ]:
            with self.subTest(text=text):
                node, conn, pipeline = self.make_node()
                script = self.write_script("build.sh", text + '\n')
                with self.assertRaisesRegex(ValueError, expected):
                    node.exec_script(script)
                self.assert_no_remote_operations(conn)

    def test_boolean_assignments_and_glob_expansions_are_not_constants(self):
        for text in [
            'DEP=present.sh\nfalse && DEP=other.sh\nsource "$DEP"',
            'DEP=present.sh\ntrue || DEP=other.sh\nsource "$DEP"',
            'DEP="*.sh"\nsource $DEP',
        ]:
            with self.subTest(text=text):
                node, conn, pipeline = self.make_node()
                script = self.write_script("build.sh", text + '\n')
                self.write_script("present.sh", 'echo present\n')
                with self.assertRaisesRegex(ValueError, "unresolved"):
                    node.exec_script(script)
                self.assert_no_remote_operations(conn)

    def test_conditional_source_does_not_invalidate_unchanged_constants(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '''#!/bin/bash
DIR=lib
if true; then
    source "$DIR/one.sh"
    source "$DIR/two.sh"
fi
''')
        self.write_script("lib/one.sh", 'echo one\n')
        self.write_script("lib/two.sh", 'echo two\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).splitlines(), ["one", "two"])

    def test_nix_environment_arguments_and_workdir_context_remain_available(self):
        node, conn, pipeline = self.make_node()
        script = self.write_script("build.sh", '#!/bin/sh\necho "$1"\n')
        with node.dir("project"):
            original_cwd = node.cwd
            expected_root = node.scriptdir
            with node.nixenv("/flake", system="aarch64-linux", name="dev", options="--impure"):
                result = node.exec_script(script, '"arg with spaces"', {"A": "B"})
            self.assertEqual(result, "ok")
            self.assertEqual(node.cwd, original_cwd)
            self.assertEqual(conn.exec_calls[-1], (
                'nix develop /flake#devShells.aarch64-linux.dev --impure --log-format raw -c '
                + shlex.quote(str(expected_root / "build.sh")) + ' "arg with spaces"',
                {"A": "B"}, str(expected_root),
            ))
            node.exec("pwd")
            self.assertEqual(conn.exec_calls[-1], ("pwd", None, str(original_cwd)))

    def test_shell_arguments_environment_result_and_execution_failure(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '#!/bin/sh\nprintf "%s:%s\\n" "$1" "$VALUE"\npwd\n')
        result = node.exec_script(script, shlex.quote("arg with spaces"), {"VALUE": "env value"})
        self.assertEqual(str(result).splitlines(), ["arg with spaces:env value", str(Path(node.scriptdir).resolve())])
        failing = self.write_script("fail.sh", '#!/bin/sh\nexit 7\n')
        with self.assertRaises(CommandError):
            node.exec_script(failing)
        node.exec("pwd")
        self.assertEqual(conn.exec_calls[-1][2], str(node.cwd))

    def test_arrays_arithmetic_and_case_patterns_are_not_script_commands(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '''#!/bin/bash
items=(source missing-array.sh bash missing-bash.sh)
count=0
((count += 1))
case "$count" in
    0) echo zero;;
    source) echo source-pattern;;
    *) source actual.sh;;
esac
''')
        self.write_script("actual.sh", 'echo actual\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).strip(), "actual")
        self.assertEqual(self.uploaded(conn), {"build.sh", "actual.sh"})

    def test_non_shell_entry_script_retains_existing_execution_behavior(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.py", '#!/usr/bin/env python3\nsource = "ordinary Python variable"\nprint(source)\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).strip(), "ordinary Python variable")
        self.assertEqual(self.uploaded(conn), {"build.py"})

    def test_redirections_and_multiple_heredocs_preserve_actual_reference(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '''#!/bin/bash
cat <<FIRST <<'SECOND' >/dev/null
source missing-first.sh
FIRST
source missing-second.sh
SECOND
2>/dev/null source 123 >result
cat result
''')
        self.write_script("123", 'echo numeric-name\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).strip(), "numeric-name")
        self.assertEqual(self.uploaded(conn), {"build.sh", "123"})

    def test_relative_entry_paths_and_direct_variable_commands(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        self.write_script("build.sh", '#!/bin/bash\nCHILD=./child.sh\n"$CHILD"\n')
        self.write_script("child.sh", '#!/bin/sh\necho child\n')
        previous = os.getcwd()
        try:
            os.chdir(self.root)
            result = node.exec_script("build.sh")
        finally:
            os.chdir(previous)
        self.assertEqual(str(result).strip(), "child")
        self.assertEqual(self.uploaded(conn), {"build.sh", "child.sh"})

    def test_symlink_outside_root_and_unsafe_directory_expression_are_rejected(self):
        node, conn, pipeline = self.make_node()
        outside = self.root.parent / "outside.sh"
        outside.write_text('echo outside\n')
        (self.root / "linked.sh").symlink_to(outside)
        script = self.write_script("build.sh", 'source linked.sh\n')
        with self.assertRaisesRegex(ValueError, r"build.sh:1:.*outside script root"):
            node.exec_script(script)
        self.assert_no_remote_operations(conn)

    def test_unrelated_command_substitution_is_never_executed_during_parsing(self):
        node, conn, pipeline = self.make_node()
        marker = self.root / "side effect"
        script = self.write_script("build.sh", 'IGNORED=$(touch ' + shlex.quote(str(marker)) + ')\necho ok\n')
        node.exec_script(script)
        self.assertFalse(marker.exists())
        self.assertEqual(self.uploaded(conn), {"build.sh"})

    def test_direct_subdirectory_path_without_dot_prefix_is_delivered(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '#!/bin/sh\nlib/child.sh\n')
        self.write_script("lib/child.sh", '#!/bin/sh\necho child\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).strip(), "child")
        self.assertEqual(self.uploaded(conn), {"build.sh", "lib/child.sh"})

    def test_unsupported_variable_mutations_invalidate_previous_constants(self):
        for mutation in [
            'DEP+=.bak', 'DEP[0]=other.sh', 'declare -n DEP=OTHER',
            'printf -v DEP %s other.sh', 'eval "DEP=other.sh"',
            'source /remote/config.sh',
        ]:
            with self.subTest(mutation=mutation):
                node, conn, pipeline = self.make_node()
                script = self.write_script("build.sh", 'DEP=present.sh\n' + mutation + '\nsource "$DEP"\n')
                self.write_script("present.sh", 'echo present\n')
                with self.assertRaisesRegex(ValueError, r"build.sh:3:.*unresolved"):
                    node.exec_script(script)
                self.assert_no_remote_operations(conn)

    def test_unquoted_directory_substitutions_check_globs_consistently(self):
        temp = tempfile.TemporaryDirectory(prefix="wildcard-")
        self.addCleanup(temp.cleanup)
        wildcard_root = Path(temp.name) / "root*"
        wildcard_root.mkdir()
        (wildcard_root / "common.sh").write_text('echo common\n')
        for expression in ['$(dirname "$0")/common.sh', '`dirname "$0"`/common.sh']:
            with self.subTest(expression=expression):
                node, conn, pipeline = self.make_node()
                script = wildcard_root / "build.sh"
                script.write_text('source ' + expression + '\n')
                with self.assertRaisesRegex(ValueError, "unresolved"):
                    node.exec_script(script)
                self.assert_no_remote_operations(conn)

    def test_quoted_tilde_is_literal_but_unquoted_tilde_is_remote(self):
        node, conn, pipeline = self.make_node()
        script = self.write_script("build.sh", 'source ~/remote.sh\nsource "~/local.sh"\n')
        self.write_script("~/local.sh", 'echo literal-tilde\n')
        node.exec_script(script)
        self.assertEqual(self.uploaded(conn), {"build.sh", "~/local.sh"})

    def test_constant_path_commands_are_distinguished_from_path_environment_commands(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '''#!/bin/bash
COMMAND=echo
"$COMMAND" ordinary
CHILD=./child.sh
"$CHILD"
SOURCE=source
"$SOURCE" common.sh
''')
        self.write_script("child.sh", '#!/bin/sh\necho child\n')
        self.write_script("common.sh", 'echo common\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).splitlines(), ["ordinary", "child", "common"])
        self.assertEqual(self.uploaded(conn), {"build.sh", "child.sh", "common.sh"})

    def test_simple_backtick_directory_expression_inside_double_quotes(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '#!/bin/bash\nHERE="`dirname "$0"`"\nsource "$HERE/common.sh"\n')
        self.write_script("common.sh", 'echo common\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).strip(), "common")
        self.assertEqual(self.uploaded(conn), {"build.sh", "common.sh"})

    def test_shell_test_builtins_do_not_look_like_dynamic_script_paths(self):
        node, conn, pipeline = self.make_node(real_shell=True)
        script = self.write_script("build.sh", '''#!/bin/bash
if [ -f build.sh ]; then source common.sh; fi
if [[ -f build.sh ]]; then source common.sh; fi
''')
        self.write_script("common.sh", 'echo common\n')
        result = node.exec_script(script)
        self.assertEqual(str(result).splitlines(), ["common", "common"])
        self.assertEqual(self.uploaded(conn), {"build.sh", "common.sh"})
