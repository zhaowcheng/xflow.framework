import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import click
from click.testing import CliRunner

from xflow.framework.main import RunGroup, main


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
        self.drop_pipeline_modules()

    def tearDown(self):
        os.chdir(self.old_cwd)
        self.drop_pipeline_modules()

    def drop_pipeline_modules(self):
        for name in list(sys.modules):
            if name == "pipelines" or name.startswith("pipelines."):
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
