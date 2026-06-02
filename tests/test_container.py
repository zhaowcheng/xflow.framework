import unittest

from xflow.framework.container import ContainerConnection


class DummyContainer:
    name = "created"


class ContainerConnectionTest(unittest.TestCase):
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
