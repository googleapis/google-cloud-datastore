# Convenience to run tests and coverage.

GAE=	/usr/local/google_appengine
GAEPATH=$(GAE):$(GAE)/lib/yaml/lib

test:
	for i in `find . -name \*_test.py`; \
	do \
	  PYTHONPATH=$(GAEPATH):. python -m ndb.`basename $$i .py` -v; \
	done

c cov cove cover coverage:
	coverage erase
	for i in `find . -name \*_test.py`; \
	do \
	  PYTHONPATH=$(GAEPATH):. coverage run -p $$i; \
	done
	coverage combine
	coverage html ndb/*.py
	coverage report -m ndb/*py
	echo "open file://`pwd`/htmlcov/index.html"

run serve:
	dev_appserver.py .

deploy:
	appcfg.py update .
