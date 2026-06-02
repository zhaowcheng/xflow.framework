import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from xflow.framework.pipeline import Pipeline


class FakeNode:
    def __init__(self, is_container=False, existed=True):
        self.is_container = is_container
        self.existed = existed
        self.calls = []

    def mkcwd(self):
        self.calls.append(("mkcwd",))

    def rmcwd(self):
        self.calls.append(("rmcwd",))

    def remove(self, force=False):
        self.calls.append(("remove", force))


class FakeEnv:
    def __init__(self, node):
        self.node = node
        self.requested = []

    def get_node(self, name):
        self.requested.append(name)
        return self.node


class MinimalPipeline(Pipeline):
    def stage1(self):
        pass


class PipelineTest(unittest.TestCase):
    def make_pipeline(self, pipeline_cls=MinimalPipeline, node=None, projdir=None):
        node = node or FakeNode()
        tmpdir = None
        if projdir is None:
            tmpdir = tempfile.TemporaryDirectory()
            self.addCleanup(tmpdir.cleanup)
            projdir = tmpdir.name
        pipeline = pipeline_cls(projdir, FakeEnv(node), "local", pipeline_cls.Options())
        return pipeline, node

    def test_option_sets_pydantic_field_metadata(self):
        class Options(Pipeline.Options):
            mode: str = Pipeline.Option(desc="Build mode", default="fast", choices=("fast", "slow"))

        field = Options.model_fields["mode"]

        self.assertEqual(Options().mode, "fast")
        self.assertEqual(field.description, "Build mode")
        self.assertEqual(
            field.json_schema_extra["typed-settings"]["click"]["type"].choices,
            ("fast", "slow"),
        )

    def test_setup_allocates_incrementing_build_ids_and_workdirs(self):
        with tempfile.TemporaryDirectory() as projdir:
            first, first_node = self.make_pipeline(projdir=projdir)
            first.setup()

            second, second_node = self.make_pipeline(projdir=projdir)
            second.setup()

            idfile = Path(projdir).joinpath("workdir", "MinimalPipeline", "buildid.txt")
            self.assertEqual(first.buildid, 1)
            self.assertEqual(second.buildid, 2)
            self.assertEqual(idfile.read_text(), "2")
            self.assertTrue(first.cwd.is_dir())
            self.assertTrue(second.cwd.is_dir())
            self.assertEqual(first_node.calls, [("mkcwd",)])
            self.assertEqual(second_node.calls, [("mkcwd",)])

    def test_stages_discovers_numbered_methods_only(self):
        class WithStages(Pipeline):
            def stage1(self):
                pass

            def stage2(self):
                pass

            def stagex(self):
                pass

        pipeline, _ = self.make_pipeline(WithStages)

        self.assertEqual(pipeline.stages, ["stage1", "stage2"])

    def test_run_success_executes_lifecycle_and_cleans_remote_workdir(self):
        class OrderedPipeline(Pipeline):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.events = []

            def setup(self):
                self.events.append("setup")
                self.buildid = 9

            def stage2(self):
                self.events.append("stage2")

            def stage1(self):
                self.events.append("stage1")

            def teardown(self):
                self.events.append("teardown")
                super().teardown()

        pipeline, node = self.make_pipeline(OrderedPipeline)

        self.assertEqual(pipeline.run(), "SUCCESSFUL")
        self.assertEqual(pipeline.events, ["setup", "stage1", "stage2", "teardown"])
        self.assertEqual(node.calls, [("rmcwd",)])

    def test_run_failure_skips_later_stages_and_does_not_cleanup_remote_workdir(self):
        class FailingPipeline(Pipeline):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.events = []

            def setup(self):
                self.events.append("setup")
                self.buildid = 9

            def stage1(self):
                self.events.append("stage1")
                raise RuntimeError("boom")

            def stage2(self):
                self.events.append("stage2")

            def teardown(self):
                self.events.append("teardown")
                super().teardown()

        pipeline, node = self.make_pipeline(FailingPipeline)

        with patch("xflow.framework.pipeline.traceback.print_exc"):
            self.assertEqual(pipeline.run(), "FAILED")
        self.assertEqual(pipeline.events, ["setup", "stage1", "teardown"])
        self.assertEqual(node.calls, [])

    def test_successful_teardown_removes_temporary_container(self):
        pipeline, node = self.make_pipeline(node=FakeNode(is_container=True, existed=False))
        pipeline.result = "SUCCESSFUL"

        pipeline.teardown()

        self.assertEqual(node.calls, [("remove", True)])

    def test_successful_teardown_keeps_existing_container_and_removes_workdir(self):
        pipeline, node = self.make_pipeline(node=FakeNode(is_container=True, existed=True))
        pipeline.result = "SUCCESSFUL"

        pipeline.teardown()

        self.assertEqual(node.calls, [("rmcwd",)])

    def test_failed_teardown_does_not_remove_anything(self):
        pipeline, node = self.make_pipeline(node=FakeNode(is_container=True, existed=False))
        pipeline.result = "FAILED"

        pipeline.teardown()

        self.assertEqual(node.calls, [])
