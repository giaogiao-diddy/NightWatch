from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parent.parent
ROOT_STR = str(ROOT)

if ROOT_STR not in sys.path:
    sys.path.insert(0, ROOT_STR)


from nightwatch.agent.graph import shutdown_graph_runtime


@pytest.fixture(autouse=True)
def _cleanup_graph_runtime() -> None:
    yield
    shutdown_graph_runtime()