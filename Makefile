UV_DEV = uv run --group dev

install:
	uv pip install -e .

test:
	$(UV_DEV) pytest

coverage:
	$(UV_DEV) pytest --cov=kubetimer --cov-report=term-missing

format-check:
	$(UV_DEV) black --check --diff kubetimer tests

format:
	$(UV_DEV) black kubetimer tests

lint:
	$(UV_DEV) flake8 kubetimer tests

typecheck:
	$(UV_DEV) mypy kubetimer

check: format-check lint typecheck test

fix: format lint