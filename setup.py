# Build hook: read version from xflow/framework/version.py,
# generate long_description from README files.
# All other metadata lives in pyproject.toml.
import re

from setuptools import setup


_VERSION_FILE = "xflow/framework/version.py"


def _find_version() -> str:
    with open(_VERSION_FILE, encoding="utf8") as f:
        m = re.search(r'''__version__\s*=\s*['"]([^'"]+)''', f.read())
    return m.group(1) if m else "0.0.0"


def _build_long_description(version: str) -> str:
    with open("README.md", encoding="utf8") as f:
        desc = f.read()
    desc = desc.replace("/blob/master/", f"/blob/v{version}/")
    desc = desc.replace("/tree/master/", f"/tree/v{version}/")
    return desc


_VERSION = _find_version()

setup(
    version=_VERSION,
    long_description=_build_long_description(_VERSION),
    long_description_content_type="text/markdown",
)
