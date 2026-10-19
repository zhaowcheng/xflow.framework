import sys
import unittest
from pathlib import Path


if __name__ == '__main__':
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    suite = unittest.defaultTestLoader.discover(
        start_dir=str(root / 'tests'),
        top_level_dir=str(root)
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
