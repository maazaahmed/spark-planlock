# Contributing to spark-planlock

Thanks for helping! Issues and PRs are welcome.

## Dev setup

```bash
git clone <your fork>
cd spark-planlock
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                    # Spark tests are skipped automatically without pyspark + Java
pytest -m "not spark"     # fast, text-only tests
ruff check src tests && ruff format --check src tests && mypy src
```

Spark integration tests need Java 17+ and `pyspark`.

## Most wanted contributions

1. **Real plan fixtures** from more environments (Spark 3.3/3.4, Databricks runtimes, EMR, Glue, Photon, Spark Connect). Run `python scripts/capture_fixtures.py` with the target Spark installed and commit the new `tests/fixtures/real/spark-<version>/` folder. If the parser disagrees, open an issue with the plan text.
2. **New lint rules.** Add a function decorated with `@rule("PL0xx")` in `src/planlock/lint.py`, give it a test with a real plan, and add it to the README table.
3. **New assertions** in `src/planlock/expect.py`. Keep them chainable (return `self`) and fail through `self._fail(message, nodes)` so the offending operators are highlighted.

## Guidelines

* The core must keep **zero runtime dependencies**. `pyspark` is only imported lazily in `_capture.py`.
* Everything except capture should work on plain text, so it's testable without Spark.
* Normalization must never hide something cost-relevant (join strategy, partitioning, filters, partition counts).
* Keep Python 3.9 compatibility.

## Releasing

1. Bump `__version__` in `src/planlock/__init__.py` and update `CHANGELOG.md`.
2. Tag `vX.Y.Z` and push; the `release` workflow builds and publishes to PyPI via trusted publishing.
