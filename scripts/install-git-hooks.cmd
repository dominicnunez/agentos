@echo off
setlocal

for /f "delims=" %%I in ('git rev-parse --show-toplevel 2^>nul') do set "AGENTOS_REPOSITORY_ROOT=%%I"
if not defined AGENTOS_REPOSITORY_ROOT (
  echo Run this script from inside the Agent OS repository.
  exit /b 1
)

git -C "%AGENTOS_REPOSITORY_ROOT%" config core.hooksPath .githooks
if errorlevel 1 exit /b %errorlevel%

echo Agent OS Git hooks enabled for this checkout.
