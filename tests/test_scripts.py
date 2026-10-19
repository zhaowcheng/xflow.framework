import copy
import os
import shlex
import tempfile
import unittest
from pathlib import Path, PurePosixPath
from unittest.mock import patch

from xflow.framework.errors import CommandError
from xflow.framework.node import Node
from xflow.framework.pipeline import Pipeline
from xflow.framework.ssh import CommandResult
from tests.test_node import FakeConnection, FakeEnv
from tests.project_imports import use_project


class DummyPipeline(Pipeline):
    def stage1(self, argstr='', envs=None):
        return self.scripts.prepare_release(argstr, envs)


class ScriptsTest(unittest.TestCase):
    def make_pipeline(
        self, script='build.sh', dependencies=None, files=None,
        quote_label=False, project=None
    ):
        if project is None:
            directory = tempfile.TemporaryDirectory()
            self.addCleanup(directory.cleanup)
            project = directory.name
        project = Path(project)
        root = project.joinpath('scripts')
        root.mkdir(parents=True)
        parameters = "label, envs=None" if quote_label else "argstr='', envs=None"
        argument = 'shlex.quote(label)' if quote_label else 'argstr'
        root.joinpath('__init__.py').write_text(
            'import shlex\n'
            'from xflow.framework.scripts import Scripts as BaseScripts\n\n'
            'class Scripts(BaseScripts):\n'
            f'    DEPS = {dependencies if dependencies is not None else {}!r}\n\n'
            f'    def prepare_release(self, {parameters}):\n'
            f'        return self.__exec({script!r}, {argument}, envs)\n'
        )
        for path, text in (files or {}).items():
            local = root.joinpath(path)
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_text(text)
        connection = FakeConnection()
        node = Node('local', 'builder', '/base', connection)
        use_project(self, project)
        pipeline = DummyPipeline(project, FakeEnv(node), 'local', DummyPipeline.Options())
        pipeline.buildid = 42
        return pipeline, node, connection

    def assert_no_delivery(self, connection):
        self.assertEqual(connection.putfile_calls, [])
        self.assertEqual(connection.exec_calls, [])

    def test_stage_calls_named_project_entry_and_returns_command_result(self):
        pipeline, node, connection = self.make_pipeline(files={'build.sh': 'echo ok'})

        result = pipeline.stage1('arg1 "arg 2"', {'MODE': 'release'})

        self.assertIsInstance(result, CommandResult)
        self.assertEqual(result.rc, 0)
        self.assertEqual(connection.uploaded_text, {
            '/base/DummyPipeline/42/scripts/build.sh': 'echo ok',
        })
        self.assertIn((
            './build.sh arg1 "arg 2"',
            {'MODE': 'release'}, '/base/DummyPipeline/42/scripts',
        ), connection.exec_calls)
        self.assertEqual(node.cwd, PurePosixPath('/base/DummyPipeline/42'))

    def test_recursive_dependencies_are_deduplicated_without_mutating_deps(self):
        declaration = {
            'build.sh': ['left.sh', 'right.sh', 'left.sh'],
            'left.sh': ['common/shared.sh'],
            'right.sh': ['common/shared.sh'],
            'unrelated.sh': ['missing.sh'],
        }
        files = {path: path for path in [
            'build.sh', 'left.sh', 'right.sh', 'common/shared.sh',
        ]}
        pipeline, _, connection = self.make_pipeline(
            dependencies=declaration, files=files
        )
        before = copy.deepcopy(pipeline.scripts.DEPS)
        connection.existing.add('/base/DummyPipeline/42/scripts/common/shared.sh')

        pipeline.stage1()

        self.assertEqual(connection.uploaded_text, {
            f'/base/DummyPipeline/42/scripts/{path}': path for path in files
        })
        self.assertEqual(len(connection.putfile_calls), 4)
        self.assertEqual(connection.exists_calls, [
            '/base/DummyPipeline/42/scripts/build.sh',
        ])
        self.assertEqual(pipeline.scripts.DEPS, before)
        executions = [call[0] for call in connection.exec_calls
                      if not call[0].startswith(('mkdir ', 'chmod '))]
        self.assertEqual(executions, ['./build.sh '])

    def test_first_delivery_preserves_nixenv_and_restores_outer_directory(self):
        pipeline, node, connection = self.make_pipeline(files={'build.sh': 'echo ok'})

        with node.dir('/outer'):
            with node.nixenv('/flake', name='dev', options='--impure'):
                result = pipeline.stage1('arg1 "arg 2"', {'A': 'B'})
            node.exec('pwd')

        expected = ('nix develop /flake#dev --impure --log-format raw -c '
                    './build.sh arg1 "arg 2"')
        self.assertEqual(result.cmd, expected)
        self.assertIn((expected, {'A': 'B'}, '/outer/scripts'), connection.exec_calls)
        self.assertIn((
            'nix develop /flake#dev --impure --log-format raw -c '
            'mkdir -p /outer/scripts', None, '/outer',
        ), connection.exec_calls)
        self.assertIn((
            'nix develop /flake#dev --impure --log-format raw -c '
            'chmod +x /outer/scripts/build.sh', None, '/outer',
        ), connection.exec_calls)
        self.assertIn(('pwd', None, '/outer'), connection.exec_calls)

    def test_initialization_does_not_check_deps_or_touch_remote_directories(self):
        pipeline, _, connection = self.make_pipeline(dependencies=['invalid'])

        self.assertEqual(pipeline.scripts.DEPS, ['invalid'])
        self.assertEqual(connection.exists_calls, [])
        self.assert_no_delivery(connection)

    def test_unrelated_declarations_are_not_validated(self):
        pipeline, _, connection = self.make_pipeline(
            dependencies={
                'build.sh': ['helper.sh'],
                '../unused.sh': None,
                42: 'unused',
            },
            files={'build.sh': 'target', 'helper.sh': 'helper'},
        )

        pipeline.stage1()

        self.assertEqual(connection.uploaded_text, {
            '/base/DummyPipeline/42/scripts/build.sh': 'target',
            '/base/DummyPipeline/42/scripts/helper.sh': 'helper',
        })

    def test_script_links_are_uploaded_without_path_checks(self):
        pipeline, _, connection = self.make_pipeline(
            dependencies={'build.sh': ['helper.sh']},
        )
        for name, content in [('build.sh', 'target'), ('helper.sh', 'helper')]:
            source = pipeline.projdir.joinpath(name)
            source.write_text(content)
            pipeline.projdir.joinpath('scripts', name).symlink_to(source)

        pipeline.stage1()

        self.assertEqual(connection.uploaded_text, {
            '/base/DummyPipeline/42/scripts/build.sh': 'target',
            '/base/DummyPipeline/42/scripts/helper.sh': 'helper',
        })

    def test_repeated_dependencies_do_not_upload_the_target_twice(self):
        pipeline, _, connection = self.make_pipeline(
            dependencies={
                'build.sh': ['helper.sh', 'helper.sh', 'other.sh'],
                'helper.sh': ['other.sh'],
            },
            files={'build.sh': 'target', 'helper.sh': 'helper', 'other.sh': 'other'},
        )

        pipeline.stage1()

        self.assertEqual(connection.uploaded_text, {
            '/base/DummyPipeline/42/scripts/build.sh': 'target',
            '/base/DummyPipeline/42/scripts/helper.sh': 'helper',
            '/base/DummyPipeline/42/scripts/other.sh': 'other',
        })
        self.assertEqual(len(connection.putfile_calls), 3)

    def test_empty_or_unreachable_deps_deliver_only_target(self):
        for declaration in [{}, {'unrelated.sh': ['missing.sh']}, {
            'missing.sh': ['other.sh'], 'other.sh': ['missing.sh'],
            'self.sh': ['self.sh'],
        }]:
            with self.subTest(declaration=declaration):
                pipeline, _, connection = self.make_pipeline(
                    dependencies=declaration, files={'build.sh': 'echo ok'}
                )

                pipeline.stage1()

                self.assertEqual(connection.uploaded_text, {
                    '/base/DummyPipeline/42/scripts/build.sh': 'echo ok',
                })

    def test_existing_target_skips_invalid_deps_and_missing_local_files(self):
        for declaration in [[], {'build.sh': ['missing.sh']},
                            {'unrelated.sh': ['../outside.sh']}]:
            with self.subTest(declaration=declaration):
                pipeline, _, connection = self.make_pipeline(dependencies=declaration)
                target = '/base/DummyPipeline/42/scripts/build.sh'
                connection.existing.add(target)

                pipeline.stage1('input.txt', {'A': 'B'})

                self.assertEqual(connection.putfile_calls, [])
                self.assertEqual(connection.exec_calls, [
                    ('./build.sh input.txt', {'A': 'B'}, '/base/DummyPipeline/42/scripts'),
                ])

    def test_first_delivery_then_reuse_skips_changed_deps_and_local_files(self):
        pipeline, _, connection = self.make_pipeline(
            dependencies={'build.sh': ['helper.sh']},
            files={'build.sh': 'echo ok', 'helper.sh': 'helper'},
        )
        pipeline.stage1('first')
        pipeline.projdir.joinpath('scripts', 'helper.sh').unlink()
        pipeline.projdir.joinpath('scripts', 'build.sh').unlink()
        type(pipeline.scripts).DEPS = ['invalid']
        connection.exec_calls.clear()
        connection.putfile_calls.clear()

        pipeline.stage1('second')

        self.assertEqual(connection.putfile_calls, [])
        self.assertEqual(connection.exec_calls, [
            ('./build.sh second', None,
             '/base/DummyPipeline/42/scripts'),
        ])

    def test_subdirectories_and_local_source_survive_directory_changes(self):
        pipeline, node, connection = self.make_pipeline(
            script='build/run.sh', files={'build/run.sh': 'target'}
        )
        oldcwd = Path.cwd()
        with tempfile.TemporaryDirectory() as other:
            try:
                os.chdir(other)
                with node.dir('source'):
                    pipeline.stage1()
                    node.exec('pwd')
            finally:
                os.chdir(oldcwd)

        self.assertEqual(connection.uploaded_text, {
            '/base/DummyPipeline/42/source/scripts/build/run.sh': 'target',
        })
        self.assertIn((
            './build/run.sh ', None,
            '/base/DummyPipeline/42/source/scripts',
        ), connection.exec_calls)
        self.assertIn(('pwd', None, '/base/DummyPipeline/42/source'), connection.exec_calls)

    def test_same_instance_reads_current_scriptdir_and_restores_after_failure(self):
        pipeline, node, connection = self.make_pipeline(files={'build.sh': 'target'})
        with node.dir('/first'):
            pipeline.stage1()
            node.exec('pwd')
        connection.failing_commands.add('./build.sh fail')
        with node.dir('/second'):
            with self.assertRaisesRegex(CommandError, 'build.sh fail'):
                pipeline.stage1('fail')
            node.exec('pwd')
        node.exec('pwd')

        self.assertEqual(connection.uploaded_text, {
            '/first/scripts/build.sh': 'target', '/second/scripts/build.sh': 'target',
        })
        for directory in ['/first', '/second', '/base/DummyPipeline/42']:
            self.assertIn(('pwd', None, directory), connection.exec_calls)
        self.assertEqual(connection.current_dir, '')

    def test_run_reports_failed_when_script_command_raises(self):
        pipeline, _, connection = self.make_pipeline(files={'build.sh': 'target'})
        connection.failing_commands.add('./build.sh ')

        with patch('xflow.framework.pipeline.traceback.print_exc'):
            result = pipeline.run()

        self.assertEqual(result, 'FAILED')
        self.assertEqual(connection.current_dir, '')
        self.assertNotIn(('rm -rf /base/DummyPipeline/1', None, ''), connection.exec_calls)

    def test_old_node_script_entry_is_removed(self):
        self.assertFalse(hasattr(Node, 'exec_script'))

    def test_project_entry_assembles_shell_quotes_for_business_argument(self):
        pipeline, _, connection = self.make_pipeline(
            files={'build.sh': 'target'}, quote_label=True
        )

        result = pipeline.stage1('release 1; echo later', {'MODE': 'release'})

        self.assertEqual(result.cmd,
                         "./build.sh 'release 1; echo later'")
        self.assertEqual(shlex.split(result.cmd), [
            './build.sh', 'release 1; echo later',
        ])
        self.assertIn((result.cmd, {'MODE': 'release'},
                       '/base/DummyPipeline/42/scripts'), connection.exec_calls)

    def test_legacy_json_file_is_not_used_for_dependency_declarations(self):
        pipeline, _, connection = self.make_pipeline(files={
            'build.sh': 'target', 'deps.json': '{invalid json',
        })

        pipeline.stage1()

        self.assertEqual(connection.uploaded_text, {
            '/base/DummyPipeline/42/scripts/build.sh': 'target',
        })

    def test_direct_dependencies_keep_same_names_in_distinct_subdirectories(self):
        files = {
            'build/run.sh': 'target', 'common/helper.sh': 'common helper',
            'other/helper.sh': 'other helper',
        }
        pipeline, _, connection = self.make_pipeline(
            script='build/run.sh', files=files,
            dependencies={'build/run.sh': [
                'common/helper.sh', 'other/helper.sh', 'common/helper.sh',
            ]},
        )
        connection.existing.add('/base/DummyPipeline/42/scripts/common/helper.sh')

        pipeline.stage1()

        self.assertEqual(connection.uploaded_text, {
            f'/base/DummyPipeline/42/scripts/{path}': text
            for path, text in files.items()
        })
        self.assertEqual(len(connection.putfile_calls), 3)
        self.assertEqual(connection.exists_calls, [
            '/base/DummyPipeline/42/scripts/build/run.sh',
        ])

    def test_relative_project_keeps_its_source_after_local_directory_change(self):
        oldcwd = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            start = Path(directory).joinpath('start')
            other = Path(directory).joinpath('other')
            start.mkdir()
            other.joinpath('project', 'scripts').mkdir(parents=True)
            other.joinpath('project', 'scripts', 'build.sh').write_text('wrong project')
            try:
                os.chdir(start)
                pipeline, _, connection = self.make_pipeline(
                    project='project', files={'build.sh': 'correct project'}
                )
                os.chdir(other)

                pipeline.stage1()
            finally:
                os.chdir(oldcwd)

        self.assertEqual(connection.uploaded_text, {
            '/base/DummyPipeline/42/scripts/build.sh': 'correct project',
        })
