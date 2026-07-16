@echo off
setlocal
set ROOT=%~dp0
if exist "%ROOT%.venv\Scripts\python.exe" (
    "%ROOT%.venv\Scripts\python.exe" "%ROOT%run_demo.py"
) else if exist "%ROOT%.venv-1\Scripts\python.exe" (
    "%ROOT%.venv-1\Scripts\python.exe" "%ROOT%run_demo.py"
) else (
    py "%ROOT%run_demo.py"
)
exit /b %ERRORLEVEL%
