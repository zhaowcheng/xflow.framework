import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from xflow.framework.env import Env
from xflow.framework.errors import NoSuchDockerError, NoSuchNodeError


class FakeNode:
    kind = "base"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.name = kwargs["name"]


class FakeNativeNode(FakeNode):
    kind = "native"


class FakeContainerNode(FakeNode):
    kind = "container"


class EnvTest(unittest.TestCase):
    def write_env(self, content):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        envfile = Path(tmpdir.name).joinpath("env.yml")
        envfile.write_text(textwrap.dedent(content), encoding="utf8")
        return envfile

    def test_loads_native_and_container_nodes(self):
        envfile = self.write_env(
            """
            nodes:
              - name: native
                docker:
                ip: 192.0.2.10
                sshport: 22
                user: builder
                password: secret
                workdir: /work/native
                envs:
                  A: B
              - name: container
                docker: local-docker
                user: root
                workdir: /work/container
                container: buildbox
                image:
                runargs: {}
                envs:
                  C: D
            dockers:
              - name: local-docker
                ip: 127.0.0.1
                port: 2375
                tls:
                  cacert:
                  clientcert:
                  clientkey:
            """
        )

        with patch("xflow.framework.env.NativeNode", FakeNativeNode), patch(
            "xflow.framework.env.ContainerNode", FakeContainerNode
        ):
            env = Env(envfile)

        native = env.get_node("native")
        container = env.get_node("container")

        self.assertIsInstance(native, FakeNativeNode)
        self.assertEqual(native.kwargs["ip"], "192.0.2.10")
        self.assertEqual(native.kwargs["envs"], {"A": "B"})

        self.assertIsInstance(container, FakeContainerNode)
        self.assertEqual(container.kwargs["ip"], "127.0.0.1")
        self.assertEqual(container.kwargs["container"], "buildbox")
        self.assertIsNone(container.kwargs["image"])
        self.assertEqual(container.kwargs["envs"], {"C": "D"})

    def test_missing_docker_reference_raises(self):
        envfile = self.write_env(
            """
            nodes:
              - name: container
                docker: missing
                user: root
                workdir: /work/container
                container: buildbox
                image:
                runargs: {}
                envs: {}
            dockers: []
            """
        )

        with patch("xflow.framework.env.ContainerNode", FakeContainerNode):
            with self.assertRaisesRegex(NoSuchDockerError, "missing"):
                Env(envfile)

    def test_get_node_raises_for_unknown_name(self):
        envfile = self.write_env(
            """
            nodes:
              - name: native
                docker:
                ip: 192.0.2.10
                sshport: 22
                user: builder
                password: secret
                workdir: /work/native
                envs: {}
            dockers: []
            """
        )

        with patch("xflow.framework.env.NativeNode", FakeNativeNode):
            env = Env(envfile)

        with self.assertRaisesRegex(NoSuchNodeError, "unknown"):
            env.get_node("unknown")
