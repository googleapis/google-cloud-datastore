# Convenience to run tests and coverage.

# You must have installed the App Engine SDK toolkit, version 1.4.0 or
# later, and it must be installed in /usr/local/google_appengine.

# This probably won't work on Windows.

FLAGS=
GAE=	/usr/local/google_appengine
GAEPATH=$(GAE):$(GAE)/lib/yaml/lib:$(GAE)/lib/webob
TESTS=	`find ndb -name [a-z]\*_test.py`
NONTESTS=`find ndb -name [a-z]\*.py ! -name \*_test.py`
PORT=	8080
ADDRESS=localhost
PYTHON= python -Wignore

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

query_test:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -m ndb.query_test $(FLAGS)

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

c cov cove cover coverage:
	coverage erase
	for i in $(TESTS); \
	do \
	  echo $$i; \
	  PYTHONPATH=$(GAEPATH):. coverage run -p $$i; \
	done
	coverage combine
	coverage html $(NONTESTS)
	coverage report -m $(NONTESTS)
	echo "open file://`pwd`/htmlcov/index.html"

serve:
	$(GAE)/dev_appserver.py . --port $(PORT) --address $(ADDRESS) $(FLAGS)

debug:
	$(GAE)/dev_appserver.py . --port $(PORT) --address $(ADDRESS) --debug $(FLAGS)

deploy:
	$(GAE)/appcfg.py update . $(FLAGS)

bench:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) bench.py $(FLAGS)

python:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) -i startup.py $(FLAGS)

python_raw:
	PYTHONPATH=$(GAEPATH):. $(PYTHON) $(FLAGS)

zip:
	D=`pwd`; D=`basename $$D`; cd ..; rm -f $$D.zip; zip $$D.zip `hg st -c -m -a -n -X $$D/.idea $$D`

clean:
	rm -rf htmlcov .coverage
	rm -f `find . -name \*.pyc -o -name \*~ -o -name @\* -o -name \*.orig -o -name \*.rej -o -name \#*\#`
