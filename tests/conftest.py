import os

import pytest

from smartdialer.store import SQLiteStore


@pytest.fixture
def store(tmp_path):
    path = str(tmp_path / "test.db")
    s = SQLiteStore(path)
    yield s
    s.close()
