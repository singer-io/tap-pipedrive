import sys
from unittest.mock import MagicMock


if "pendulum" not in sys.modules:
    sys.modules["pendulum"] = MagicMock()
