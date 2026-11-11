import os
import subprocess
import sys
import pytest

pytestmark = pytest.mark.skipif(os.getenv("RUN_CHAOS_TEST") != "1", reason="Destructive 10k-job chaos test is opt-in")


def test_full_chaos_harness():
    result = subprocess.run([sys.executable, "scripts/run_chaos_test.py"], check=False)
    assert result.returncode == 0
