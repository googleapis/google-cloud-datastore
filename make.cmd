@ECHO OFF

REM Convenience to run tests on Windows.

REM You must have installed the App Engine SDK toolkit, version 1.5.4 or
REM later, and it must be installed in the default location.

REM Requires that an official Python installer in the 2.5 series is used and
REM that Python is installed in the default location.

REM TODO: Support versions 2.6 and 2.7
SET PYTHONVER=25

SET FLAGS=
SET PORT=8080
SET ADDRESS=localhost

REM Find Google App Engine
SET GAESUBDIR=Google\google_appengine
IF EXIST "%PROGRAMFILES(x86)%" SET PROGRAMFILES=%PROGRAMFILES(x86)%
SET GAE=%PROGRAMFILES%\%GAESUBDIR%
IF EXIST "%GAE%" GOTO gaefound
ECHO Could not find App Engine in %GAE%
GOTO end

:gaefound
REM Google App Engine Variables
SET GAEPATH=%GAE%;%GAE%\lib\yaml\lib;%GAE%\lib\webob
SET APPCFG="%GAE%\appcfg.py"
SET DEV_APPSERVER="%GAE%\dev_appserver.py"

REM Find Python
SET PYTHON=C:\Python%PYTHONVER%\python.exe
IF EXIST %PYTHON% GOTO pythonfound
ECHO Could not find python.exe in C:\Python%PYTHONVER%\
GOTO end

:pythonfound
REM Python Variables
SET PYTHON=%PYTHON% -Wignore
SET PYTHONPATH=%GAEPATH%

REM Arguments to perform actions
IF "%1"=="key_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="model_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="query_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="rpc_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="eventloop_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="tasklets_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="context_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="thread_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="runtests" %PYTHON% %1.py %FLAGS%
REM TODO: Implement coverage
IF "%1"=="serve" START %PYTHON% %DEV_APPSERVER% . --port %PORT% --address %ADDRESS% %FLAGS%
IF "%1"=="debug" START %PYTHON% %DEV_APPSERVER% . --port %PORT% --address %ADDRESS% --debug %FLAGS%
IF "%1"=="deploy" %PYTHON% %APPCFG% update . %FLAGS%
IF "%1"=="bench" %PYTHON% %1.py %FLAGS%
IF "%1"=="keybench" %PYTHON% %1.py %FLAGS%
IF "%1"=="python" %PYTHON% -i startup.py %FLAGS%
IF "%1"=="python_raw" %PYTHON% %FLAGS%
REM TODO: Implement zip
REM TODO: Implement clean

:end
