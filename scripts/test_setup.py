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
  "compose -f $SETUP_TEST_COMPOSE pull postgres redis") exit 0 ;;
  "compose -f $SETUP_TEST_COMPOSE up "*)
    for argument do service=$argument; done
    [ "$PWD" = "$SETUP_TEST_REPO" ] && [ "$SETUP_TEST_FAIL" != up ] && [ "$SETUP_TEST_FAIL" != "$service" ] ;;
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
        if shell and shutil.which("git"):
            checks += checkBootstrap(root, shell, runners, stub, folder)
        else:
            print("Bootstrap checks require Git and a POSIX shell.")
        print(f"{checks} setup checks passed: {', '.join(name for name, _ in runners)}")


def checkBootstrap(root, shell, runners, stub, folder):
    """Use real local Git remotes, with no network, to verify safe updates."""
    source = folder / "local upstream"
    source.mkdir()
    env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
               GIT_AUTHOR_NAME="Setup Test", GIT_AUTHOR_EMAIL="setup@example.invalid",
               GIT_COMMITTER_NAME="Setup Test", GIT_COMMITTER_EMAIL="setup@example.invalid")

    def git(*args, cwd=source):
        return subprocess.check_output(["git", *args], cwd=cwd, env=env,
                                       stderr=subprocess.PIPE, text=True).strip()

    git("init", "-b", "main")
    for file in ("Dockerfile", "package-lock.json", "master.mdb"):
        (source / file).write_text("initial\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "Initial test checkout")
    repos = {"umamoe-resources": "umamoe-resources", "umamoe_db": "umamoe-search",
             "umamoe-frontend": "umamoe-frontend", "umamoe-embeds": "umamoe-embeds"}
    checks = 0
    for name, runner in runners:
        for case in ("clone", "private", "all_private", "missing_master", "existing",
                     "resources", "search", "frontend", "embeds", "up"):
            with tempfile.TemporaryDirectory(prefix="bootstrap case ", dir=folder) as temporary:
                parent = Path(temporary)
                repo = parent / "umamoe-backend"
                repo.mkdir()
                for script in ("setup.sh", "setup.cmd"):
                    shutil.copyfile(root / script, repo / script)
                original_heads = {}
                if case in ("existing", "missing_master"):
                    for directory in repos:
                        git("clone", str(source), str(parent / directory))
                        original_heads[directory] = git("rev-parse", "HEAD", cwd=parent / directory)
                if case == "missing_master":
                    (parent / "umamoe-resources/master.mdb").unlink()
                if case == "existing":
                    (parent / "umamoe_db/Dockerfile").write_text("local edits\n", encoding="utf-8")
                    git("branch", "--unset-upstream", cwd=parent / "umamoe-frontend")
                    (parent / "umamoe-embeds/local.txt").write_text("local commit\n", encoding="utf-8")
                    git("add", "local.txt", cwd=parent / "umamoe-embeds")
                    git("commit", "-m", "Local work", cwd=parent / "umamoe-embeds")
                    original_heads["umamoe-embeds"] = git("rev-parse", "HEAD", cwd=parent / "umamoe-embeds")
                    (source / "update.txt").write_text(name, encoding="utf-8")
                    git("add", "update.txt")
                    git("commit", "-m", "Upstream work")

                log = parent / "calls.log"
                log.touch()
                test_env = dict(env, PATH=os.pathsep.join([str(stub), str(Path(shell).parent), os.environ["PATH"]]),
                                SETUP_TEST_LOG=str(log), SETUP_TEST_REPO=str(repo),
                                SETUP_TEST_COMPOSE="compose.services.yml", SETUP_TEST_FAIL=case,
                                GIT_CONFIG_COUNT=str(len(repos)))
                for index, remote in enumerate(repos.values()):
                    unavailable = case == "all_private" or (case == "private" and remote == "umamoe-embeds")
                    target = parent / "unavailable" if unavailable else source
                    test_env[f"GIT_CONFIG_KEY_{index}"] = f"url.{target.as_uri()}.insteadOf"
                    test_env[f"GIT_CONFIG_VALUE_{index}"] = f"https://github.com/uma-moe/{remote}.git"
                if os.name == "nt":
                    test_env["SETUP_TEST_REPO"] = subprocess.check_output(
                        [shell, "-c", 'cd "$SETUP_TEST_REPO" && pwd'], env=test_env, text=True
                    ).strip()
                result = subprocess.run(runner + [str(repo / name), "bootstrap"], cwd=parent,
                                        env=test_env, capture_output=True, text=True, timeout=60)
                output = result.stdout + result.stderr
                calls = log.read_text(encoding="utf-8")
                assert (result.returncode == 0) == (case != "up"), (name, case, output)
                if case == "up":
                    assert "--no-deps" not in calls and "Demo ready:" not in output, output
                    checks += 1
                    continue
                assert "Bootstrap started: postgres redis backend" in output, (name, case, output)
                assert "--wait-timeout 600 backend" in calls, calls
                if case == "existing":
                    assert git("rev-parse", "HEAD", cwd=parent / "umamoe-resources") == git("rev-parse", "HEAD")
                    for directory in ("umamoe_db", "umamoe-frontend", "umamoe-embeds"):
                        assert git("rev-parse", "HEAD", cwd=parent / directory) == original_heads[directory]
                    assert (parent / "umamoe_db/Dockerfile").read_text() == "local edits\n"
                    assert "local changes present" in output and "requires a merge" in output, output
                if case in ("all_private", "missing_master", "resources"):
                    assert "--no-deps search" not in calls, calls
                if case in ("private", "all_private", "frontend"):
                    assert "--no-deps embeds" not in calls, calls
                if case in ("resources", "search", "missing_master", "private"):
                    assert "--no-deps frontend" in calls and "Frontend:" in output, output
                if case in ("resources", "search", "frontend", "embeds"):
                    assert f"Could not start {case}; continuing" in output, output
                if case == "all_private":
                    assert "--no-deps" not in calls and "Could not clone" in output, output
                if case == "clone":
                    assert all((parent / directory / ".git").is_dir() for directory in repos)
                    assert "--no-deps embeds" in calls, calls
                    assert git("remote", "get-url", "origin", cwd=parent / "umamoe_db") == "https://github.com/uma-moe/umamoe-search.git"
                checks += 1
    return checks


if __name__ == "__main__":
    main()
