build-image:
	docker build . -t gusty-testing

run-image:
	docker run -d -v ${GUSTY_DEV_HOME}:/gusty --name gusty-testing gusty-testing

exec:
	docker exec -it gusty-testing /bin/bash

start-container:
	docker start gusty-testing

stop-container:
	docker stop gusty-testing

test:
	docker run --rm -v ${GUSTY_DEV_HOME}:/gusty --name gusty-make-test gusty-testing pytest

lint:
	docker run --rm -v ${GUSTY_DEV_HOME}:/gusty --name gusty-make-lint gusty-testing flake8

coverage:
	docker run --rm -v ${GUSTY_DEV_HOME}:/gusty --name gusty-make-lint gusty-testing pytest --cov=gusty --cov-report=html tests/

browse-coverage:
	see htmlcov/index.html

fmt:
	black .
	flake8
