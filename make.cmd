@ECHO OFF

REM Convenience to run tests on Windows.
REM
REM You must have installed the App Engine SDK toolkit, version 1.5.4 or later.
REM
REM An optional second argument specifies the Python version as a two digit
REM number (2.6.5 == 26), the default is 25 (as in 2.5.x).
REM
REM Environment variables that override defaults:
REM  - PYTHON: path to a specific python.exe to use
REM  - PYTHON25, PYTHON26, PYTHON27: same as PYTHON but for individual versions
REM  - PYTHONFLAGS: flags to pass python, defaults to -Wignore
REM  - PYTHONFLAGS25, PYTHONFLAGS26, PYTHONFLAGS27: same as PYTHONFLAGS but for
REM                                                 individual versions
REM  - GAE: path to the google_appengine directory
REM  - FLAGS: flags to pass either python.exe or dev_appserver.py/appcfg.py
REM  - PORT: port number for development server to use
REM  - PORT25, PORT26, PORT27: same as PORT but for individual versions
REM  - ADDRESS: IP address/hostname for development server to use

:start
SETLOCAL
SET SUPPORTED_VERSIONS=(25 26 27)

:processarguments
IF NOT "%3"=="" (
  ECHO Invalid argument %3
  GOTO end
)
SET MAKE=%0
SET VERSION=%2
FOR /F "TOKENS=1,2,3 DELIMS=_" %%A IN ("%1") DO SET T1=%%A&SET T2=%%B&SET T3=%%C
SET TARGET=%T1%
IF NOT "%T3%"=="" (
  SET TARGET=%TARGET%_%T2%
  SET T2=%T3%
)
IF /I "%T2%"=="all" (
  IF "%VERSION%"=="" GOTO all
  ECHO Cannot specify Python version %VERSION% if using an all target
  GOTO end
)
IF NOT "%T2%"=="" SET TARGET=%TARGET%_%T2%

:detectpythonversion
IF "%VERSION%"=="" SET VERSION=25
REM TODO: Do not rely on pythonxx batch labels
FOR %%A IN %SUPPORTED_VERSIONS% DO IF "%VERSION%"=="%%A" GOTO python%%A
ECHO Will not work with Python version %VERSION%
GOTO end

:python25
IF NOT "%PYTHON25%"=="" SET PYTHON=%PYTHON25%
IF NOT "%PYTHONFLAGS25%"=="" SET PYTHONFLAGS=%PYTHONFLAGS25%
IF NOT "%PORT25%"=="" SET PORT=%PORT25%
GOTO findpython

:python26
IF NOT "%PYTHON26%"=="" SET PYTHON=%PYTHON26%
IF NOT "%PYTHONFLAGS26%"=="" SET PYTHONFLAGS=%PYTHONFLAGS26%
IF NOT "%PORT26%"=="" SET PORT=%PORT26%
GOTO findpython

:python27
IF NOT "%PYTHON27%"=="" SET PYTHON=%PYTHON27%
IF NOT "%PYTHONFLAGS27%"=="" SET PYTHONFLAGS=%PYTHONFLAGS27%
IF NOT "%PORT27%"=="" SET PORT=%PORT27%
GOTO findpython

:findpython
IF "%PYTHON%"=="" SET PYTHON=C:\Python%VERSION%\python.exe
IF EXIST "%PYTHON%" GOTO findgae
ECHO Could not find python executable %PYTHON%
GOTO end

:findgae
IF NOT "%PROGRAMFILES(x86)%"=="" SET PROGRAMFILES=%PROGRAMFILES(x86)%
IF "%GAE%"=="" SET GAE=%PROGRAMFILES%\Google\google_appengine
IF EXIST "%GAE%" GOTO setvariables
ECHO Could not find App Engine in %GAE%
GOTO end

:setvariables
IF "%PYTHONFLAGS%"=="" SET PYTHONFLAGS=-Wignore
SET PYTHONPATH=%GAE%;%GAE%\lib\yaml\lib;%GAE%\lib\webob
SET APPCFG="%GAE%\appcfg.py"
SET DEV_APPSERVER="%GAE%\dev_appserver.py"
IF "%PORT%"=="" SET PORT=8080
IF "%ADDRESS%"=="" SET ADDRESS=localhost

:findtarget
SET TEST_TARGETS=(key, model, query, rpc, eventloop, tasklets, context, thread)
FOR %%A IN %TEST_TARGETS% DO IF /I "%TARGET%"=="%%A_test" GOTO runtest
SET RUNTESTS_TARGETS=(test, runtest, runtests)
FOR %%A IN %RUNTESTS_TARGETS% DO IF /I "%TARGET%"=="%%A" GOTO runtests
REM TODO: Implement coverage
SET COVERAGE_TARGETS%=(c, cov, cove, cover, coverage)
FOR %%A IN %COVERAGE_TARGETS% DO IF /I "%TARGET%"=="%%A" GOTO unimplemented
IF /I "%TARGET%"=="serve" GOTO serve
IF /I "%TARGET%"=="debug" GOTO debug
IF /I "%TARGET%"=="deploy" GOTO deploy
SET ONEOFF_TARGETS%=(bench, gettaskletrace, keybench)
FOR %%A IN %ONEOFF_TARGETS% DO IF /I "%TARGET%"=="%%A" GOTO oneoff
IF /I "%TARGET%"=="python" GOTO python
IF /I "%TARGET%"=="python_raw" GOTO pythonraw
REM TODO: Implement zip
IF /I "%TARGET%"=="zip" GOTO unimplemented
IF /I "%TARGET%"=="clean" GOTO clean
SET LONGLINES_TARGETS%=(long, longline, longlines)
FOR %%A IN %LONGLINES_TARGETS% DO IF /I "%TARGET%"=="%%A" GOTO longlines
SET TRIM_TARGETS%=(tr, trim, trim_whitespace)
FOR %%A IN %TRIM_TARGETS% DO IF /I "%TARGET%"=="%%A" GOTO trim
IF "%TARGET%"=="" (
  ECHO Must specify a make target e.g. serve
) ELSE (
  IF NOT EXIST %TARGET% (
    ECHO Invalid target %TARGET%
  ) ELSE (
    CALL %PYTHON% %PYTHONFLAGS% %TARGET% %FLAGS%
  )
)
GOTO end

:runtest
IF "%VERSION%"=="25" (
  ECHO %TARGET% only supports Python 2.6 or above due to relative imports
  ECHO Note that the runtests target can perform this test using Python 2.5
) ELSE (
  CALL %PYTHON% %PYTHONFLAGS% -m ndb.%TARGET% %FLAGS%
)
GOTO end

:runtests
CALL %PYTHON% %PYTHONFLAGS% runtests.py %FLAGS%
GOTO end

:debug
SET FLAGS=%FLAGS% --debug

:serve
SET SERVE=%DEV_APPSERVER% . --port %PORT% --address %ADDRESS%
IF EXIST %DEV_APPSERVER% START %PYTHON% %PYTHONFLAGS% %SERVE% %FLAGS%
IF NOT EXIST %DEV_APPSERVER% ECHO Could not find dev_appserver.py in %GAE%
GOTO end

:deploy
IF EXIST %APPCFG% CALL %PYTHON% %PYTHONFLAGS% %APPCFG% update . %FLAGS%
IF NOT EXIST %APPCFG% ECHO Could not find appcfg.py in %GAE%
GOTO end

:oneoff
CALL %PYTHON% %PYTHONFLAGS% %TARGET%.py %FLAGS%
GOTO end

:python
SET PYTHONFLAGS=%PYTHONFLAGS% -i startup.py

:pythonraw
CALL %PYTHON% %PYTHONFLAGS% %FLAGS%
GOTO end

:clean
RMDIR /S /Q htmlcov .coverage > NUL 2>&1
DEL /S *.pyc *~ @* *.orig *.rej #*# > NUL 2>&1
GOTO end

:trim
CALL %PYTHON% %PYTHONFLAGS% trimwhitespace.py %FLAGS%
GOTO end

:longlines
CALL %PYTHON% %PYTHONFLAGS% longlines.py %FLAGS%
GOTO end

:all
REM TODO: Should only fail once if target is invalid/unimplemented
FOR %%A IN %SUPPORTED_VERSIONS% DO CALL %MAKE% %TARGET% %%A
GOTO end

:unimplemented
ECHO %TARGET% unimplemented. If you implement it, please submit a patch to:
ECHO http://code.google.com/p/appengine-ndb-experiment/issues/detail?id=56

:end
ENDLOCAL
