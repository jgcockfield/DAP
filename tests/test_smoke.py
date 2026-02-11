import subprocess
import sys

def test_run_daily_dry_run_smoke():
    r = subprocess.run(
        [sys.executable, "-m", "dap.run_daily", "--dry-run", "--limit", "1"],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stdout + "\n" + r.stderr
