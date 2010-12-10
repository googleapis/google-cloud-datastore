# Convenience to run tests and coverage.

FLAGS=
GAE=	/usr/local/google_appengine
GAEPATH=$(GAE):$(GAE)/lib/yaml/lib
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
	$(GAE)/dev_appserver.py . --port $(PORT) --address $(ADDRESS)

debug:
	$(GAE)/dev_appserver.py . --port $(PORT) --address $(ADDRESS) --debug

deploy:
	appcfg.py update .

python:
	PYTHONPATH=$(GAEPATH):. $(PYTHON)

clean:
	rm -rf htmlcov
	rm -f `find . -name \*.pyc -o -name \*~ -o -name @* -o -name \*.orig`
