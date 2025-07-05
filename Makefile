SOURCE_FILES = src

.PHONY: format
format:
	uv run ruff check ${SOURCE_FILES} --fix --unsafe-fixes
	uv run ruff format ${SOURCE_FILES}

.PHONY: lint
lint:
	uv run mypy ${SOURCE_FILES}
	uv run pyright ${SOURCE_FILES}
	uv run ruff check ${SOURCE_FILES}
	uv run ruff format --check ${SOURCE_FILES}
