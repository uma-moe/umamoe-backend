@echo off
setlocal DisableDelayedExpansion
pushd "%~dp0" || exit /b 1

where docker >nul 2>&1
if errorlevel 1 (
    echo Install Docker Desktop, then run this script again. >&2
    goto failed
)
call docker compose version >nul 2>&1
if errorlevel 1 (
    echo Docker Compose is required. Install or update Docker Desktop, then retry. >&2
    goto failed
)
call docker info >nul 2>&1
if errorlevel 1 (
    echo Start Docker Desktop using Linux containers, then run this script again. >&2
    goto failed
)

echo Starting the standalone demo. The first Rust build can take several minutes.
call docker compose -f compose.local.yml up --build -d --wait --wait-timeout 180
if errorlevel 1 (
    echo Setup failed. Check: docker compose -f compose.local.yml logs --tail=100 >&2
    goto failed
)

set "demoAddress="
for /f "delims=" %%A in ('docker compose -f compose.local.yml port backend 3001') do set "demoAddress=%%A"
if not defined demoAddress goto failed
echo.
echo Demo ready: http://%demoAddress%/api/health
echo API key: uma_demo_key_001
echo Local login token: docker compose -f compose.local.yml run --rm --no-deps demo-db --token
echo Stop: docker compose -f compose.local.yml down
popd
exit /b 0

:failed
popd
exit /b 1
