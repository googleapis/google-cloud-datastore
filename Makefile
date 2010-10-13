# Convenience to run tests and coverage.

FLAGS=
GAE=	/usr/local/google_appengine
GAEPATH=$(GAE):$(GAE)/lib/yaml/lib
TESTS=	`find ndb -name \*_test.py`
PORT=	8080
ADDRESS=localhost

test:
	for i in $(TESTS); \
	do \
	  PYTHONPATH=$(GAEPATH):. python -m ndb.`basename $$i .py` $(FLAGS); \
	done

c cov cove cover coverage:
	coverage erase
	for i in $(TESTS); \
	do \
	  PYTHONPATH=$(GAEPATH):. coverage run -p $$i; \
	done
	coverage combine
	coverage html ndb/*.py
	coverage report -m ndb/*py
	echo "open file://`pwd`/htmlcov/index.html"

serve:
	dev_appserver.py . --port $(PORT) --address $(ADDRESS)

deploy:
	appcfg.py update .
