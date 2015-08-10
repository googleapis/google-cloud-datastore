# Convenience to run tests and coverage.

# You must have installed the App Engine SDK toolkit in
# /usr/local/google_appengine.  For the required version see README.

# For Windows users, the "make.cmd" script has similar functionality.

FLAGS=
export GAE?=	/usr/local/google_appengine
GAEPATH=$(GAE):$(GAE)/lib/yaml/lib:$(GAE)/lib/webob:$(GAE)/lib/fancy_urllib:$(GAE)/lib/simplejson:$(GAE)/lib/protorpc:$(GAE)/lib/protorpc-1.0
TESTS=	`find ndb -name [a-z]\*_test.py ! -name ndb_test.py`
NONTESTS=`find ndb -name [a-z]\*.py ! -name \*_test.py`
PORT=	8080
HOST=localhost
PYTHON= python -Wignore
APPCFG= $(GAE)/appcfg.py
DEV_APPSERVER=$(GAE)/dev_appserver.py
CUSTOM=	custom
COVERAGE=coverage
DATASTORE_PATH=/tmp/ndb-dev_appserver.datastore
APP_ID=
APP_VERSION=

serve:
	$(PYTHON) $(DEV_APPSERVER) demo/ --port $(PORT) --host $(HOST) $(FLAGS) --datastore_path=$(DATASTORE_PATH)

debug:
	$(PYTHON) $(DEV_APPSERVER) demo/ --port $(PORT) --host $(HOST) --debug $(FLAGS) --datastore_path=$(DATASTORE_PATH)

deploy:
	$(PYTHON) $(APPCFG) update demo/ $(FLAGS)

bench:
	$(PYTHON) tests/bench.py $(FLAGS)

keybench:
	$(PYTHON) tests/keybench.py $(FLAGS)

gettaskletrace:
	$(PYTHON) tests/gettaskletrace.py $(FLAGS)

stress:
	$(PYTHON) tests/stress.py $(FLAGS)

race:
	$(PYTHON) tests/race.py $(FLAGS)

mttest:
	$(PYTHON) tests/mttest.py $(FLAGS)
