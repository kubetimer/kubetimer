install:
	uv pip install -e .

test:
	uv run --group dev pytest

coverage:
	uv run --group dev pytest --cov=kubetimer --cov-report=term-missing