# Convenience to run tests and coverage.

# You must have installed the App Engine SDK toolkit in
# /usr/local/google_appengine.  For the required version see README.

# For Windows users, the "make.cmd" script has similar functionality.

FLAGS=
GAE=	/usr/local/google_appengine
GAEPATH=$(GAE):$(GAE)/lib/yaml/lib:$(GAE)/lib/webob:$(GAE)/lib/fancy_urllib:$(GAE)/lib/simplejson
TESTS=	`find ndb -name [a-z]\*_test.py`
NONTESTS=`find ndb -name [a-z]\*.py ! -name \*_test.py`
PORT=	8080
ADDRESS=localhost
PYTHON= python -Wignore
APPCFG= $(GAE)/appcfg.py
DEV_APPSERVER=$(GAE)/dev_appserver.py
CUSTOM=	custom
COVERAGE=coverage
DATASTORE_PATH=/tmp/ndb-dev_appserver.datastore

default: runtests

test:
	for i in $(TESTS); \
	do \
	  echo $$i; \
	  PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.`basename $$i .py` $(FLAGS); \
	done

key_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.key_test $(FLAGS)

model_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.model_test $(FLAGS)

polymodel_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.polymodel_test $(FLAGS)

query_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.query_test $(FLAGS)

metadata_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.metadata_test $(FLAGS)

stats_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.stats_test $(FLAGS)

rpc_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.rpc_test $(FLAGS)

eventloop_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.eventloop_test $(FLAGS)

tasklets_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.tasklets_test $(FLAGS)

context_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.context_test $(FLAGS)

thread_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.thread_test $(FLAGS)

ps_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.prospective_search_test $(FLAGS)

blobstore_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.blobstore_test $(FLAGS)


runtests:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) runtests.py $(FLAGS)

c cov cove cover coverage:
	PYTHONPATH=$(GAEPATH):. $(COVERAGE) run runtests.py $(FLAGS)
	$(COVERAGE) html $(NONTESTS)
	$(COVERAGE) report -m $(NONTESTS)
	echo "open file://`pwd`/htmlcov/index.html"

oldcoverage:
	$(COVERAGE) erase
	for i in $(TESTS); \
	do \
	  echo $$i; \
	  PYTHONPATH=$(GAEPATH):. $(COVERAGE) run -p -m ndb.`basename $$i .py`; \
	done
	$(COVERAGE) combine
	$(COVERAGE) html $(NONTESTS)
	$(COVERAGE) report -m $(NONTESTS)
	echo "open file://`pwd`/htmlcov/index.html"

serve:
	$(PYTHON) $(DEV_APPSERVER) . --port $(PORT) --address $(ADDRESS) $(FLAGS) --datastore_path=$(DATASTORE_PATH)

debug:
	$(PYTHON) $(DEV_APPSERVER) . --port $(PORT) --address $(ADDRESS) --debug $(FLAGS) --datastore_path=$(DATASTORE_PATH)

deploy:
	$(PYTHON) $(APPCFG) update . $(FLAGS)

bench:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) bench.py $(FLAGS)

keybench:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) keybench.py $(FLAGS)

python:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -i startup.py $(FLAGS)

python_raw:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) $(FLAGS)

longlines:
	$(PYTHON) longlines.py

tr trim trimwhitespace:
	$(PYTHON) trimwhitespace.py

zip:
	D=`pwd`; D=`basename $$D`; cd ..; rm -f $$D.zip; zip $$D.zip `hg st -c -m -a -n -X $$D/.idea $$D`

clean:
	rm -rf htmlcov .coverage
	rm -f `find . -name \*.pyc -o -name \*~ -o -name @\* -o -name \*.orig -o -name \*.rej -o -name \#*\#`

g gettaskletrace:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) gettaskletrace.py $(FLAGS)

s stress:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) stress.py $(FLAGS)

race:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) race.py $(FLAGS)

$(CUSTOM):
	PYTHONPATH=$(GAEPATH):. $(PYTHON) $(CUSTOM).py $(FLAGS)
