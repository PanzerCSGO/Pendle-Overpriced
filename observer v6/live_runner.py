# FILE: live_runner.py
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime, timezone

from config import Config
cfg = Config()

TS_FMT = "%Y-%m-%d_%H-%M-%S"


def safe_load_json(path: Path, default):
    try:
        if not path.exists() or path.stat().st_size == 0:
            return default
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def run_cmd(cmd, name):
    print(f"[runner] starting {name}: {' '.join(cmd)}")
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def tail_process(proc, name, max_lines=200):
    lines = []
    try:
        while True:
            line = proc.stdout.readline()
            if not line:
                break
            line = line.rstrip("\n")
            lines.append(line)
            if len(lines) > max_lines:
                lines = lines[-max_lines:]
            # print live
            print(f"[{name}] {line}")
    except Exception:
        pass
    return lines


def main():
    base = Path(__file__).resolve().parent
    collector_py = base / "collector" / "Pendle_snapshot_collector.py"
    observer_py = base / "observer_v2_complete.py"
    exec_py = base / "execution_bot.py"

    if not collector_py.exists():
        raise SystemExit(f"collector not found: {collector_py}")
    if not observer_py.exists():
        raise SystemExit(f"observer not found: {observer_py}")
    if not exec_py.exists():
        raise SystemExit(f"execution_bot not found: {exec_py}")

    # Start collector + observer + exec in a loop (simple, robust).
    # Collector polls every 10 min by default.
    # Observer/exec run every 10 min here as well (aligned).
    poll = 600

    print(f"[runner] live runner started at {now_iso()} (poll={poll}s)")
    while True:
        # 1) run observer
        obs = run_cmd(["python", str(observer_py), "default"], "observer")
        tail_process(obs, "observer")
        obs.wait()

        # 2) run execution bot (candidate emitter)
        ex = run_cmd(["python", str(exec_py)], "exec")
        tail_process(ex, "exec")
        ex.wait()

        # 3) sleep until next cycle
        print(f"[runner] sleep {poll}s ...")
        time.sleep(poll)


if __name__ == "__main__":
    main()
