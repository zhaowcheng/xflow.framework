import os
import re
import shlex
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import click
from click.testing import CliRunner

from xflow.framework.main import RunGroup, main
from xflow.framework.node import Node
from xflow.framework.pipeline import Pipeline
from tests.test_node import FakeConnection, FakeEnv as ScriptEnv
from tests.project_imports import use_project


class FakeNode:
    is_container = False
    existed = True

    def mkcwd(self):
        pass

    def rmcwd(self):
        pass

    def remove(self, force=False):
        pass


class FakeEnv:
    def __init__(self, envfile):
        self.envfile = envfile

    def get_node(self, name):
        return FakeNode()


class CliTest(unittest.TestCase):
    def setUp(self):
        self.old_cwd = os.getcwd()
        self.old_path = sys.path.copy()
        self.drop_pipeline_modules()

    def tearDown(self):
        os.chdir(self.old_cwd)
        sys.path[:] = self.old_path
        self.drop_pipeline_modules()

    def drop_pipeline_modules(self):
        for name in list(sys.modules):
            if name.split('.')[0] in ('pipelines', 'scripts'):
                sys.modules.pop(name)

    def test_init_copies_template_into_empty_project(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            projdir = Path(tmpdir).joinpath("project")

            result = runner.invoke(main, ["-p", str(projdir), "init"])

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertTrue(projdir.joinpath("env.yml").is_file())
            self.assertTrue(projdir.joinpath("pipelines", "example.py").is_file())
            self.assertIn(f"Initialized {projdir}", result.output)

    def test_init_rejects_non_empty_project_directory(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            projdir = Path(tmpdir).joinpath("project")
            projdir.mkdir()
            projdir.joinpath("keep.txt").write_text("data")

            result = runner.invoke(main, ["-p", str(projdir), "init"])

            self.assertEqual(result.exit_code, 1)
            self.assertIn("exists and is not empty", result.output)

    def test_init_delivers_loadable_empty_scripts(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            projdir = Path(tmpdir).joinpath('project')

            result = runner.invoke(main, ['-p', str(projdir), 'init'])

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertTrue(projdir.joinpath('scripts', '__init__.py').is_file())
            self.assertFalse(projdir.joinpath('scripts', 'deps.json').exists())
            use_project(self, projdir)
            pipeline = Pipeline(projdir, FakeEnv(None), 'local', Pipeline.Options())
            self.assertEqual(pipeline.scripts.DEPS, {})
            self.assertEqual(set(type(pipeline.scripts).__dict__) - {
                '__module__', '__doc__', 'DEPS',
            }, set())

    def test_initialized_project_can_run_the_script_guide_example(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            projdir = Path(tmpdir).joinpath('project')
            initialized = runner.invoke(main, ['-p', str(projdir), 'init'])
            self.assertEqual(initialized.exit_code, 0, initialized.output)
            guide = projdir.joinpath('README.md').read_text()
            python_examples = re.findall(r'```python\n(.*?)\n```', guide, re.S)
            shell_examples = re.findall(r'```sh\n(.*?)\n```', guide, re.S)
            root = projdir.joinpath('scripts')
            root.joinpath('__init__.py').write_text(python_examples[0])
            for name, content in zip(
                ['build/run.sh', 'common/log.sh'], shell_examples
            ):
                script = root.joinpath(name)
                script.parent.mkdir(parents=True, exist_ok=True)
                script.write_text(content)
            entry = projdir.joinpath('pipelines', 'release.py')
            entry.write_text(python_examples[1])
            cli_command = re.search(r'`(xflow [^`]+)`', guide).group(1)
            executable, *arguments = shlex.split(cli_command)
            self.assertEqual(executable, 'xflow')
            connection = FakeConnection()
            node = Node('local', 'builder', '/base', connection)
            try:
                os.chdir(projdir)
                with (
                    patch.dict(os.environ, {}, clear=True),
                    patch.object(sys, 'path', sys.path.copy()),
                    patch('xflow.framework.main.Env', return_value=ScriptEnv(node)),
                ):
                    result = runner.invoke(main, arguments)
            finally:
                os.chdir(self.old_cwd)

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(connection.uploaded_text, {
                '/base/release/1/scripts/build/run.sh': shell_examples[0],
                '/base/release/1/scripts/common/log.sh': shell_examples[1],
            })
            target = './build/run.sh'
            script_calls = [call for call in connection.exec_calls
                            if shlex.split(call[0])[0] == target]
            self.assertEqual(len(script_calls), 1)
            [(command, envs, directory)] = script_calls
            self.assertEqual(shlex.split(command), [target, "release O'Reilly"])
            self.assertEqual(envs, {'MODE': 'release'})
            self.assertEqual(directory, '/base/release/1/scripts')
            buildid = projdir.joinpath('workdir', 'release', 'buildid.txt')
            self.assertEqual(buildid.read_text(), '1')
            self.assertIn(('rm -rf /base/release/1', None, ''),
                          connection.exec_calls)

    def test_run_group_lists_pipeline_modules_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipelines = Path(tmpdir).joinpath("pipelines")
            pipelines.mkdir()
            pipelines.joinpath("__init__.py").write_text("")
            pipelines.joinpath("template.py").write_text("")
            pipelines.joinpath("demo.py").write_text("")
            pipelines.joinpath("build.py").write_text("")
            pipelines.joinpath("notes.txt").write_text("")
            group = RunGroup("run")
            ctx = click.Context(group, obj={"projdir": tmpdir})

            self.assertCountEqual(group.list_commands(ctx), ["demo", "build"])

    def test_run_executes_pipeline_command(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            projdir = Path(tmpdir)
            scripts = projdir.joinpath('scripts')
            scripts.mkdir(parents=True)
            scripts.joinpath('__init__.py').write_text(
                'from xflow.framework.scripts import Scripts as BaseScripts\n'
                'class Scripts(BaseScripts):\n    DEPS = {}\n'
            )
            pipelines = projdir.joinpath("pipelines")
            pipelines.mkdir()
            pipelines.joinpath("__init__.py").write_text("")
            pipelines.joinpath("demo.py").write_text(
                textwrap.dedent(
                    """
                    from pathlib import Path
                    from xflow.framework.pipeline import Pipeline

                    class demo(Pipeline):
                        def stage1(self):
                            Path(self.projdir).joinpath("ran.txt").write_text("ok")
                    """
                )
            )

            with patch("xflow.framework.main.Env", FakeEnv):
                result = runner.invoke(main, ["-p", str(projdir), "run", "-n", "local", "demo"])

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(projdir.joinpath("ran.txt").read_text(), "ok")

    def test_failed_pipeline_command_returns_nonzero(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            projdir = Path(tmpdir)
            scripts = projdir.joinpath('scripts')
            scripts.mkdir(parents=True)
            scripts.joinpath('__init__.py').write_text(
                'from xflow.framework.scripts import Scripts as BaseScripts\n'
                'class Scripts(BaseScripts):\n    DEPS = {}\n'
            )
            pipelines = projdir.joinpath("pipelines")
            pipelines.mkdir()
            pipelines.joinpath("__init__.py").write_text("")
            pipelines.joinpath("broken.py").write_text(
                textwrap.dedent(
                    """
                    from xflow.framework.pipeline import Pipeline

                    class broken(Pipeline):
                        def stage1(self):
                            raise RuntimeError("boom")
                    """
                )
            )

            with patch("xflow.framework.main.Env", FakeEnv):
                result = runner.invoke(main, ["-p", str(projdir), "run", "-n", "local", "broken"])

            self.assertEqual(result.exit_code, 1)
