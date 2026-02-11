import subprocess
import sys

def test_limit_flag_changes_seeded_count():
    r = subprocess.run(
        [sys.executable, "-m", "dap.run_daily", "--dry-run", "--limit", "1"],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0
    assert "seeded=1" in r.stdout
