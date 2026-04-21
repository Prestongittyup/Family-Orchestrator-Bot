up:
	docker-compose up --build

down:
	docker-compose down

test:
	docker-compose run backend pytest -q

eval:
	docker-compose run backend pytest tests/test_brief_evaluation.py -s
