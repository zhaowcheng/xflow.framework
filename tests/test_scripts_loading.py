import os
import tempfile
import traceback
import unittest
from pathlib import Path

from tests.project_imports import use_project
from tests.test_node import FakeConnection, FakeEnv
from xflow.framework.node import Node
from xflow.framework.pipeline import Pipeline


class ProjectPipeline(Pipeline):
    def stage1(self):
        return self.scripts.prepare_release()


class ScriptsLoadingTest(unittest.TestCase):
    project_source = (
        'from xflow.framework.scripts import Scripts as BaseScripts\n'
        'from .settings import TARGET, DEPS\n'
        'class Scripts(BaseScripts):\n'
        '    DEPS = DEPS\n'
        '    def prepare_release(self):\n'
        '        from .settings import LABEL\n'
        '        return self.__exec(TARGET, LABEL)\n'
    )

    def make_project(self, source, files=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        project = Path(directory.name)
        root = project.joinpath('scripts')
        root.mkdir()
        root.joinpath('__init__.py').write_text(source)
        for path, content in (files or {}).items():
            target = root.joinpath(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        use_project(self, project)
        return project

    def make_pipeline(self, project, connection=None):
        connection = connection or FakeConnection()
        node = Node('local', 'builder', '/base', connection)
        pipeline = ProjectPipeline(
            project, FakeEnv(node), 'local', ProjectPipeline.Options()
        )
        return pipeline, node, connection

    def test_project_imports_support_relative_modules_and_stage_execution(self):
        project = self.make_project(self.project_source, {
            'settings.py': (
                "TARGET = 'build.sh'\nLABEL = 'release'\n"
                "DEPS = {'build.sh': ['helper.sh']}\n"
            ),
            'build.sh': 'target',
            'helper.sh': 'helper',
        })
        pipeline, _, connection = self.make_pipeline(project)
        pipeline.buildid = 1

        result = pipeline.stage1()

        self.assertEqual(result.cmd, './build.sh release')
        self.assertEqual(connection.uploaded_text, {
            '/base/ProjectPipeline/1/scripts/build.sh': 'target',
            '/base/ProjectPipeline/1/scripts/helper.sh': 'helper',
        })

    def test_repeated_pipelines_use_the_normally_imported_project_class(self):
        project = self.make_project(
            'from xflow.framework.scripts import Scripts as BaseScripts\n'
            'class Scripts(BaseScripts):\n    DEPS = {}\n'
        )

        first, _, _ = self.make_pipeline(project)
        second, _, _ = self.make_pipeline(project)

        self.assertIs(type(first.scripts), type(second.scripts))
        self.assertIsNot(first.scripts, second.scripts)

    def test_relative_project_keeps_its_source_after_directory_changes(self):
        project = self.make_project(self.project_source, {
            'settings.py': "TARGET = 'build.sh'\nLABEL = 'original'\nDEPS = {}\n",
            'build.sh': 'original script',
        })
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as other:
            try:
                os.chdir(project.parent)
                pipeline, _, connection = self.make_pipeline(project.name)
                pipeline.buildid = 1
                os.chdir(other)

                result = pipeline.stage1()
            finally:
                os.chdir(previous)

        self.assertEqual(result.cmd, './build.sh original')
        self.assertEqual(connection.uploaded_text, {
            '/base/ProjectPipeline/1/scripts/build.sh': 'original script',
        })

    def test_empty_project_initializes_without_remote_script_operations(self):
        project = self.make_project(
            'from xflow.framework.scripts import Scripts as BaseScripts\n'
            'class Scripts(BaseScripts):\n    DEPS = {}\n'
        )

        pipeline, _, connection = self.make_pipeline(project)

        self.assertIsNone(pipeline.buildid)
        self.assertEqual(pipeline.scripts.DEPS, {})
        self.assertEqual(connection.exists_calls, [])
        self.assertEqual(connection.putfile_calls, [])
        self.assertEqual(connection.exec_calls, [])
        self.assertEqual(connection.dir_calls, [])

    def test_missing_class_propagates_normal_import_error(self):
        project = self.make_project('DEPS = {}\n')

        with self.assertRaises(ImportError) as error:
            self.make_pipeline(project)

        self.assertEqual(error.exception.name, 'scripts')

    def test_missing_package_propagates_normal_import_error(self):
        project = self.make_project('')
        root = project.joinpath('scripts')
        root.joinpath('__init__.py').unlink()
        root.rmdir()

        with self.assertRaises(ModuleNotFoundError) as error:
            self.make_pipeline(project)

        self.assertEqual(error.exception.name, 'scripts')

    def test_project_constructor_errors_propagate(self):
        project = self.make_project(
            'class Scripts:\n'
            '    def __init__(self):\n        pass\n'
        )

        with self.assertRaises(TypeError):
            self.make_pipeline(project)

    def test_syntax_error_retains_project_filename(self):
        project = self.make_project('class Scripts(\n')

        with self.assertRaises(SyntaxError) as error:
            self.make_pipeline(project)

        self.assertEqual(error.exception.filename,
                         str(project.joinpath('scripts', '__init__.py')))

    def test_internal_import_error_retains_cause_and_project_location(self):
        project = self.make_project(
            'from . import helper\n',
            {'helper.py': 'import xflow_missing_project_dependency\n'},
        )
        connection = FakeConnection()
        try:
            self.make_pipeline(project, connection)
        except ModuleNotFoundError as error:
            self.assertEqual(error.name, 'xflow_missing_project_dependency')
            frames = traceback.extract_tb(error.__traceback__)
            self.assertIn(str(project.joinpath('scripts', 'helper.py')),
                          [frame.filename for frame in frames])
        else:
            self.fail('Pipeline initialization must expose the import error')

        self.assertEqual(connection.exists_calls, [])
        self.assertEqual(connection.exec_calls, [])
