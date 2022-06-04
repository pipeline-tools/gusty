build-image:
	docker build . -t gusty-testing:latest

run-image:
	docker run -d -v ${GUSTY_DEV_HOME}:/gusty --name gusty-testing gusty-testing:latest

exec:
	docker exec -it gusty-testing /bin/bash

start-container:
	docker start gusty-testing

stop-container:
	docker stop gusty-testing

test:
	docker run --rm -v ${GUSTY_DEV_HOME}:/gusty --name gusty-make-test gusty-testing:latest pytest

lint:
	docker run --rm -v ${GUSTY_DEV_HOME}:/gusty --name gusty-make-lint gusty-testing:latest flake8

coverage:
	docker run --rm -v ${GUSTY_DEV_HOME}:/gusty --name gusty-make-lint gusty-testing:latest pytest --cov=gusty --cov-report=html tests/

browse-coverage:
	see htmlcov/index.html

fmt:
	black .
	flake8
