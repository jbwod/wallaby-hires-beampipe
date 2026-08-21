PYTHON ?= python3

.PHONY: help
help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "%-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: show
show: ## Show the Python and package versions.
	@$(PYTHON) --version
	@$(PYTHON) -m wallaby_hires --version 2>/dev/null || true

.PHONY: install
install: ## Build and force-reinstall the package (used by the DALiuGE runtime).
	$(PYTHON) -m pip install --disable-pip-version-check --force-reinstall --no-deps .

.PHONY: install-dev
install-dev: ## Install the package and development/test tooling.
	$(PYTHON) -m pip install --disable-pip-version-check -e .
	$(PYTHON) -m pip install --disable-pip-version-check \
		black build coverage flake8 isort mypy pytest pytest-cov twine types-requests

.PHONY: fmt
fmt: ## Format Python source and tests.
	$(PYTHON) -m isort wallaby_hires tests scripts
	$(PYTHON) -m black wallaby_hires tests scripts

.PHONY: lint
lint: ## Run formatting, style, and type checks.
	$(PYTHON) -m flake8 --max-line-length 90 --extend-ignore E203,E501,W503 \
		wallaby_hires tests scripts
	$(PYTHON) -m black --check wallaby_hires tests scripts
	$(PYTHON) -m isort --check-only wallaby_hires tests scripts
	$(PYTHON) -m mypy wallaby_hires

.PHONY: test
test: ## Run assertion-based tests with coverage.
	$(PYTHON) -m pytest -v --cov=wallaby_hires --cov-report=term-missing tests

.PHONY: build
build: ## Build source and wheel distributions using PEP 517.
	$(PYTHON) -m build

.PHONY: check-dist
check-dist: build ## Validate built distribution metadata.
	$(PYTHON) -m twine check dist/*

.PHONY: check
check: lint test check-dist ## Run the complete local release gate.

.PHONY: clean
clean: ## Remove generated development and distribution files.
	@find . -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	@rm -rf .coverage .pytest_cache .mypy_cache build dist htmlcov
	@rm -rf wallaby_hires.egg-info

.PHONY: release
release: ## Set one version, commit it, tag it, and push the release.
	@read -r -p "Version (x.y.z): " TAG; \
	$(PYTHON) scripts/version.py --set "$$TAG"; \
	poetry lock; \
	git add pyproject.toml poetry.lock wallaby_hires/VERSION; \
	git commit -m "release: wallaby-hires v$$TAG"; \
	git tag "v$$TAG"; \
	git push origin HEAD --tags

.PHONY: docs
docs: ## Build documentation without opening a GUI.
	$(PYTHON) -m mkdocs build --strict
