import unittest

from xflow.framework.ssh import CommandResult


class CommandResultTest(unittest.TestCase):
    def test_normalizes_output_and_keeps_metadata(self):
        result = CommandResult("\x1b[31m hello\x1b[0m\xe9\r\nworld\n", rc=7, cmd="echo hello")

        self.assertEqual(str(result), "hello\nworld")
        self.assertEqual(result.rc, 7)
        self.assertEqual(result.cmd, "echo hello")

    def test_getfield_finds_last_matching_line(self):
        result = CommandResult(
            """\
UID        PID   CMD
postgres   45    /opt/pgsql/bin/postgres
postgres   51    postgres: checkpointer process
postgres   52    postgres: writer process
"""
        )

        self.assertEqual(result.getfield("postgres", 2), "52")
        self.assertEqual(result.getfield("checkpointer", 1, sep=":"), "postgres   51    postgres")
        self.assertEqual(result.getfield(1, 2), "PID")
        self.assertIsNone(result.getfield("missing", 1))

    def test_getcol_returns_available_fields(self):
        result = CommandResult(
            """\
name: value
other: second
lonely
"""
        )

        self.assertEqual(result.getcol(1, sep=":"), ["name", "other", "lonely"])
        self.assertEqual(result.getcol(2, sep=":"), [" value", " second"])
