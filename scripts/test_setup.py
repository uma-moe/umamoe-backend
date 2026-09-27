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
  "compose -f $SETUP_TEST_COMPOSE up "*)
    [ "$PWD" = "$SETUP_TEST_REPO" ] && [ "$SETUP_TEST_FAIL" != up ] ;;
  "compose -f $SETUP_TEST_COMPOSE port backend 3001") echo '127.0.0.1:55303' ;;
  "compose -f $SETUP_TEST_COMPOSE port frontend 4200") echo '127.0.0.1:55420' ;;
  *) exit 99 ;;
esac
""", encoding="utf-8", newline="\n")
        (stub / "docker").chmod(0o755)
        (stub / "docker.cmd").write_text("""@echo off
echo %*>>"%SETUP_TEST_LOG%"
if "%~1 %~2"=="compose version" if "%SETUP_TEST_FAIL%"=="compose" exit /b 1
if "%~1"=="info" if "%SETUP_TEST_FAIL%"=="daemon" exit /b 1
if "%~2"=="-f" if not "%~3"=="%SETUP_TEST_COMPOSE%" exit /b 99
if "%~4"=="up" if not "%CD%"=="%SETUP_TEST_REPO%" exit /b 99
if "%~4"=="up" if "%SETUP_TEST_FAIL%"=="up" exit /b 1
if "%~4 %~5"=="port backend" echo 127.0.0.1:55303
if "%~4 %~5"=="port frontend" echo 127.0.0.1:55420
exit /b 0
""", encoding="utf-8", newline="\r\n")

        checks = 0
        for name, runner in runners:
            cases = [(mode, failure) for mode in ("", "services")
                     for failure in ("", "compose", "daemon", "up")]
            cases += [("services", failure) for failure in
                      ("missing_repo", "missing_master", "master_directory")]
            cases.append(("invalid", "usage"))
            for mode, failure in cases:
                required = [folder / file for file in (
                    "umamoe-resources/Dockerfile", "umamoe_db/Dockerfile",
                    "umamoe-embeds/Dockerfile", "umamoe-frontend/package-lock.json",
                    "umamoe-frontend/angular.json", "umamoe-resources/master.mdb")]
                for path in required:
                    path.parent.mkdir(exist_ok=True)
                    if path.is_dir():
                        path.rmdir()
                    path.write_text("test", encoding="utf-8")
                if failure == "missing_repo":
                    required[0].unlink()
                if failure in ("missing_master", "master_directory"):
                    required[-1].unlink()
                if failure == "master_directory":
                    required[-1].mkdir()
                log = folder / "calls.log"
                log.write_text("", encoding="utf-8")
                test_path = [str(stub)] + ([str(Path(shell).parent)] if shell else [])
                env = dict(os.environ, PATH=os.pathsep.join(test_path + [os.environ["PATH"]]),
                           SETUP_TEST_LOG=str(log), SETUP_TEST_FAIL=failure,
                           SETUP_TEST_COMPOSE="compose.services.yml" if mode == "services" else "compose.local.yml",
                           SETUP_TEST_REPO=str(repo))
                if name.endswith(".sh") and os.name == "nt":
                    env["SETUP_TEST_REPO"] = subprocess.check_output(
                        [shell, "-c", 'cd "$SETUP_TEST_REPO" && pwd'], env=env, text=True
                    ).strip()
                result = subprocess.run(runner + [str(repo / name)] + ([mode] if mode else []), cwd=folder,
                                        env=env, capture_output=True, text=True, timeout=15)
                output = result.stdout + result.stderr
                calls = log.read_text(encoding="utf-8")
                assert (result.returncode == 0) == (not failure), (name, mode, failure, output)
                assert ("Demo ready:" in output) == (not failure), (name, mode, failure, output)
                if failure and failure != "up":
                    assert " up " not in calls, calls
                if failure in ("missing_repo", "missing_master", "master_directory", "usage"):
                    assert not calls, calls
                    expected = "Missing" if failure == "missing_repo" else "Usage:" if failure == "usage" else "master.mdb"
                    assert expected in output, output
                if not failure:
                    assert "--build" in calls and "--wait" in calls, calls
                    assert "http://127.0.0.1:55303/api/health" in output, output
                    if mode == "services":
                        assert "Frontend: http://127.0.0.1:55420" in output, output
                        assert "--wait-timeout 600" in calls, calls
                        assert "compose.services.yml run --rm --no-deps demo-db --token" in output, output
                checks += 1
        print(f"{checks} setup checks passed: {', '.join(name for name, _ in runners)}")


if __name__ == "__main__":
    main()
