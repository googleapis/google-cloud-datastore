@ECHO OFF

REM Convenience to run tests on Windows.

REM You must have installed the App Engine SDK toolkit, version 1.5.4 or
REM later, and it must be installed in the default location.

REM Requires that an official Python installer in the 2.5 series is used and
REM that Python is installed in the default location.

SETLOCAL
REM TODO: Support versions 2.6 and 2.7
SET PYTHONVER=25

SET FLAGS=
SET PORT=8080
SET ADDRESS=localhost

REM Find Google App Engine
SET GAESUBDIR=Google\google_appengine
IF EXIST "%PROGRAMFILES(x86)%" SET PROGRAMFILES=%PROGRAMFILES(x86)%
SET GAE=%PROGRAMFILES%\%GAESUBDIR%
IF NOT EXIST "%GAE%" ECHO Could not find App Engine in %GAE%
IF NOT EXIST "%GAE%" GOTO end

REM Google App Engine Variables
SET GAEPATH="%GAE%;%GAE%\lib\yaml\lib;%GAE%\lib\webob"
SET APPCFG="%GAE%\appcfg.py"
SET DEV_APPSERVER="%GAE%\dev_appserver.py"

REM Find Python
SET PYTHON=C:\Python%PYTHONVER%\python.exe
IF NOT EXIST %PYTHON% ECHO Could not find python.exe in C:\Python%PYTHONVER%\
IF NOT EXIST %PYTHON% GOTO end
SET PYTHON=%PYTHON% -Wignore
SET PYTHONPATH=%GAEPATH%

REM Temp file hack so that regedit only needs run once per TEMP folder cleanup
IF EXIST %TEMP%\GAEPATHSET GOTO gaepath_set

ECHO About to set PYTHONPATH in the registry to include Google App Engine
PAUSE

REM Need current directory for using regedit
CD > %TEMP%\PWD
SET /P PWD=<%TEMP%\PWD

REM Add Google App Engine to PYTHONPATH
IF EXIST "%PROGRAMFILES(x86)%" (
  REGEDIT /S "%PWD%\google_appengine64.reg"
) ELSE (
  REGEDIT /S "%PWD%\google_appengine.reg"
)
ECHO . > %TEMP%\GAEPATHSET

:gaepath_set
REM Arguments to perform actions
IF "%1"=="key_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="model_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="query_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="rpc_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="eventloop_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="tasklets_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="context_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="thread_test" %PYTHON% -m ndb.%1 %FLAGS%
IF "%1"=="runtests" %PYTHON% runtests.py %FLAGS%
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
ENDLOCAL
