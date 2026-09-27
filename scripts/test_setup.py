"""Exercise setup success and Docker failures without starting containers.

Run: python scripts/test_setup.py
"""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    root = Path(__file__).resolve().parents[1]
    shell = shutil.which("sh")
    if not shell and os.name == "nt" and shutil.which("git"):
        candidate = Path(shutil.which("git")).resolve().parents[1] / "usr/bin/sh.exe"
        if candidate.is_file():
            shell = str(candidate)
    runners = [("setup.sh", [shell])] if shell else []
    if os.name == "nt":
        runners.append(("setup.cmd", [os.environ["COMSPEC"], "/d", "/c"]))
    assert runners, "A POSIX shell or Windows Command Prompt is required"

    with tempfile.TemporaryDirectory(prefix="umamoe setup test ") as temporary:
        folder = Path(temporary)
        repo = folder / "repo with spaces"
        stub = folder / "bin"
        repo.mkdir()
        stub.mkdir()
        for name, _ in runners:
            shutil.copyfile(root / name, repo / name)

        (stub / "docker").write_text("""#!/bin/sh
printf '%s\n' "$*" >> "$SETUP_TEST_LOG"
case "$*" in
  'compose version') [ "$SETUP_TEST_FAIL" != compose ] ;;
  info) [ "$SETUP_TEST_FAIL" != daemon ] ;;
  'compose -f compose.local.yml up '*)
    [ "$PWD" = "$SETUP_TEST_REPO" ] && [ "$SETUP_TEST_FAIL" != up ] ;;
  'compose -f compose.local.yml port backend 3001') echo '127.0.0.1:55303' ;;
  *) exit 99 ;;
esac
""", encoding="utf-8", newline="\n")
        (stub / "docker").chmod(0o755)
        (stub / "docker.cmd").write_text("""@echo off
echo %*>>"%SETUP_TEST_LOG%"
if "%~1 %~2"=="compose version" if "%SETUP_TEST_FAIL%"=="compose" exit /b 1
if "%~1"=="info" if "%SETUP_TEST_FAIL%"=="daemon" exit /b 1
if "%~4"=="up" if not "%CD%"=="%SETUP_TEST_REPO%" exit /b 99
if "%~4"=="up" if "%SETUP_TEST_FAIL%"=="up" exit /b 1
if "%~4"=="port" echo 127.0.0.1:55303
exit /b 0
""", encoding="utf-8", newline="\r\n")

        checks = 0
        for name, runner in runners:
            for failure in ("", "compose", "daemon", "up"):
                log = folder / "calls.log"
                log.write_text("", encoding="utf-8")
                test_path = [str(stub)] + ([str(Path(shell).parent)] if shell else [])
                env = dict(os.environ, PATH=os.pathsep.join(test_path + [os.environ["PATH"]]),
                           SETUP_TEST_LOG=str(log), SETUP_TEST_FAIL=failure,
                           SETUP_TEST_REPO=str(repo))
                if name.endswith(".sh") and os.name == "nt":
                    env["SETUP_TEST_REPO"] = subprocess.check_output(
                        [shell, "-c", 'cd "$SETUP_TEST_REPO" && pwd'], env=env, text=True
                    ).strip()
                result = subprocess.run(runner + [str(repo / name)], cwd=folder,
                                        env=env, capture_output=True, text=True, timeout=15)
                output = result.stdout + result.stderr
                calls = log.read_text(encoding="utf-8")
                assert (result.returncode == 0) == (not failure), (name, failure, output)
                assert ("Demo ready:" in output) == (not failure), (name, failure, output)
                if failure in ("compose", "daemon"):
                    assert " up " not in calls, calls
                if not failure:
                    assert "--build" in calls and "--wait" in calls, calls
                    assert "http://127.0.0.1:55303/api/health" in output, output
                checks += 1
        print(f"{checks} setup checks passed: {', '.join(name for name, _ in runners)}")


if __name__ == "__main__":
    main()
