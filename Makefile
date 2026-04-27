.PHONY: install test lint config-check refresh-universe

install:
	python -m pip install -e .[dev]

test:
	python -m pytest

lint:
	python -m ruff check src tests

config-check:
	iee config-check

refresh-universe:
	iee refresh-universe
