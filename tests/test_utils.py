import inspect
import unittest

from xflow.framework.utils import (
    copy_signature,
    isclass,
    remove_ansi_escape_chars,
    remove_unprintable_chars,
)


class UtilsTest(unittest.TestCase):
    def test_remove_ansi_escape_chars(self):
        self.assertEqual(remove_ansi_escape_chars("\x1b[31mhello\x1b[0m"), "hello")

    def test_remove_unprintable_chars(self):
        self.assertEqual(remove_unprintable_chars("hello\xe9\n"), "hello\n")

    def test_copy_signature_keeps_callable_behavior_and_signature(self):
        def source(first: int, second: str = "x") -> str:
            return second

        @copy_signature(source)
        def replacement(*args, **kwargs):
            return args, kwargs

        self.assertEqual(str(inspect.signature(replacement)), "(first: int, second: str = 'x') -> str")
        self.assertEqual(replacement(1, second="y"), ((1,), {"second": "y"}))

    def test_isclass(self):
        class Example:
            pass

        self.assertTrue(isclass(Example))
        self.assertFalse(isclass(Example()))
