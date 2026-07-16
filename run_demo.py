import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run_step(name: str, script: Path) -> int:
    print(f"\n=== Running {name} ===")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    completed = subprocess.run([sys.executable, str(script)], cwd=str(ROOT), env=env)
    if completed.returncode != 0:
        print(f"{name} exited with code {completed.returncode}")
        return completed.returncode
    return 0


def main() -> int:
    print("E-Commerce Real-Time Pipeline demo")
    print(f"Repository root: {ROOT}")
    steps = [
        ("Delta Lakehouse (bronze -> silver -> gold, with quarantine)",
         ROOT / "lakehouse" / "delta_lakehouse.py"),
    ]
    for name, script in steps:
        status = run_step(name, script)
        if status != 0:
            return status

    artifacts = [
        ROOT / "lakehouse" / "data" / "bronze",
        ROOT / "lakehouse" / "data" / "silver",
        ROOT / "lakehouse" / "data" / "gold",
        ROOT / "lakehouse" / "data" / "quarantine",
    ]
    print("\nGenerated Delta tables:")
    for artifact in artifacts:
        if artifact.exists():
            print(f"- {artifact.relative_to(ROOT)}")
        else:
            print(f"- Missing: {artifact.relative_to(ROOT)}")

    print("\nDemo completed. The repository is ready for GitHub upload and local review.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())