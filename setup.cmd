@echo off
setlocal DisableDelayedExpansion
pushd "%~dp0" || exit /b 1

set "composeFile=compose.local.yml"
set "waitTimeout=180"
if not "%~2"=="" goto usage
if "%~1"=="" goto dockerCheck
if not "%~1"=="services" goto usage
set "composeFile=compose.services.yml"
set "waitTimeout=600"
for %%F in (umamoe-resources/Dockerfile umamoe_db/Dockerfile umamoe-embeds/Dockerfile umamoe-frontend/package-lock.json umamoe-frontend/angular.json) do (
    if not exist "../%%F" (
        echo Missing ../%%F. Check out the required repo beside umamoe-backend; private repos require access. >&2
        goto failed
    )
)
if not exist "../umamoe-resources/master.mdb" goto missingMaster
if exist "../umamoe-resources/master.mdb/" goto missingMaster

:dockerCheck
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

echo Starting %composeFile%. The first build can take several minutes.
call docker compose -f %composeFile% up --build -d --wait --wait-timeout %waitTimeout%
if errorlevel 1 (
    echo Setup failed. Check: docker compose -f %composeFile% logs --tail=100 >&2
    goto failed
)

set "demoAddress="
for /f "delims=" %%A in ('docker compose -f %composeFile% port backend 3001') do set "demoAddress=%%A"
if not defined demoAddress goto failed
echo.
echo Demo ready: http://%demoAddress%/api/health
if "%composeFile%"=="compose.services.yml" (
    for /f "delims=" %%A in ('docker compose -f compose.services.yml port frontend 4200') do echo Frontend: http://%%A
)
echo API key: uma_demo_key_001
echo Local login token: docker compose -f %composeFile% run --rm --no-deps demo-db --token
echo Stop: docker compose -f %composeFile% down
popd
exit /b 0

:usage
echo Usage: setup.cmd [services] >&2
goto failed

:missingMaster
echo Place the game's master.mdb in ../umamoe-resources/master.mdb before starting services. >&2

:failed
popd
exit /b 1
