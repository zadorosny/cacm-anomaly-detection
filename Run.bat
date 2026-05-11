@echo off
REM CA/CM Anomaly Detection - Windows task runner.
REM Usage:
REM   Run.bat                 -> show help
REM   Run.bat install         -> install dependencies via Poetry
REM   Run.bat data            -> generate synthetic dataset
REM   Run.bat train           -> train Isolation Forest + Autoencoder
REM   Run.bat pipeline        -> run bronze -> silver -> gold + alerts
REM   Run.bat all             -> install + data + train + pipeline
REM   Run.bat test            -> run unit tests
REM   Run.bat check           -> lint + format-check + typecheck + tests
REM   Run.bat mlflow          -> launch MLflow UI on http://localhost:5000
REM   Run.bat clean-data      -> remove generated data/models/checkpoints

setlocal enabledelayedexpansion

set "TARGET=%~1"
if "%TARGET%"=="" set "TARGET=help"

REM Default pipeline parameters (override via env vars before calling Run.bat)
if "%DIAS%"==""        set "DIAS=30"
if "%AGENCIAS%"==""    set "AGENCIAS=50"
if "%ASSOCIADOS%"==""  set "ASSOCIADOS=10000"
if "%OPERADORES%"==""  set "OPERADORES=200"
if "%TX_POR_DIA%"==""  set "TX_POR_DIA=5000"
if "%SEED%"==""        set "SEED=42"
if "%EPOCHS%"==""      set "EPOCHS=40"

if /I "%TARGET%"=="help"             goto :help
if /I "%TARGET%"=="install"          goto :install
if /I "%TARGET%"=="install-pip"      goto :install_pip
if /I "%TARGET%"=="data"             goto :data
if /I "%TARGET%"=="train"            goto :train
if /I "%TARGET%"=="pipeline"         goto :pipeline
if /I "%TARGET%"=="all"              goto :all
if /I "%TARGET%"=="test"             goto :test
if /I "%TARGET%"=="test-integration" goto :test_integration
if /I "%TARGET%"=="lint"             goto :lint
if /I "%TARGET%"=="format"           goto :format
if /I "%TARGET%"=="format-check"     goto :format_check
if /I "%TARGET%"=="typecheck"        goto :typecheck
if /I "%TARGET%"=="check"            goto :check
if /I "%TARGET%"=="mlflow"           goto :mlflow
if /I "%TARGET%"=="docs"             goto :docs
if /I "%TARGET%"=="clean-data"       goto :clean_data

echo Unknown target: %TARGET%
goto :help

:help
echo.
echo CA/CM Anomaly Detection - Windows runner
echo.
echo Usage: Run.bat ^<target^>
echo.
echo Targets:
echo   install            Install runtime + dev deps via Poetry
echo   install-pip        Alternative: install via pip (no Poetry)
echo   data               Generate synthetic transactions + dimensions
echo   train              Train Isolation Forest + Autoencoder
echo   pipeline           Run bronze -^> silver -^> gold + alerts
echo   all                Full end-to-end (install + data + train + pipeline)
echo   test               Run unit tests
echo   test-integration   Run integration tests (Spark required)
echo   lint               Ruff lint
echo   format             Auto-format with ruff format
echo   format-check       Verify formatting without writing
echo   typecheck          Mypy static type-check
echo   check              Run every CI gate locally
echo   mlflow             Launch MLflow UI on http://localhost:5000
echo   docs               Serve MkDocs docs on http://localhost:8000
echo   clean-data         Remove generated parquets / models / checkpoints
echo.
echo Pipeline parameters (override before calling):
echo   set DIAS=90 ^&^& Run.bat data
echo   set EPOCHS=20 ^&^& Run.bat train
goto :eof

:install
poetry install --with dev || goto :error
poetry run pre-commit install
goto :eof

:install_pip
python -m pip install -r requirements-dev.txt || goto :error
goto :eof

:data
poetry run python scripts/gerar_dados.py ^
    --dias %DIAS% --agencias %AGENCIAS% --associados %ASSOCIADOS% ^
    --operadores %OPERADORES% --tx-por-dia %TX_POR_DIA% --seed %SEED%
goto :eof

:train
poetry run python scripts/treinar_modelos.py --epochs %EPOCHS%
goto :eof

:pipeline
poetry run python scripts/rodar_pipeline.py
goto :eof

:all
call :install   || goto :error
call :data      || goto :error
call :train     || goto :error
call :pipeline  || goto :error
goto :eof

:test
poetry run pytest -m "not integration"
goto :eof

:test_integration
poetry run pytest -m integration
goto :eof

:lint
poetry run ruff check src tests
goto :eof

:format
poetry run ruff format src tests
goto :eof

:format_check
poetry run ruff format --check src tests
goto :eof

:typecheck
poetry run mypy src
goto :eof

:check
call :lint         || goto :error
call :format_check || goto :error
call :typecheck    || goto :error
call :test         || goto :error
goto :eof

:mlflow
poetry run mlflow ui --port 5000
goto :eof

:docs
poetry run mkdocs serve
goto :eof

:clean_data
if exist data\bronze rmdir /s /q data\bronze
if exist data\silver rmdir /s /q data\silver
if exist data\gold rmdir /s /q data\gold
if exist data\checkpoints rmdir /s /q data\checkpoints
if exist data\models rmdir /s /q data\models
if exist data\landing rmdir /s /q data\landing
if exist mlruns rmdir /s /q mlruns
if exist spark-warehouse rmdir /s /q spark-warehouse
if exist metastore_db rmdir /s /q metastore_db
if exist derby.log del /q derby.log
goto :eof

:error
echo.
echo [Run.bat] Step "%TARGET%" failed with exit code %errorlevel%.
exit /b %errorlevel%
