## What does this change?

<!-- One or two sentences. Link the issue it closes, e.g. "Closes #12". -->

## Checklist

- [ ] Tests added or updated (`pytest -m "not spark"` passes; Spark tests too if you touched capture/integration)
- [ ] `ruff check src tests`, `ruff format --check src tests` and `mypy src` pass
- [ ] New lint rules / assertions are documented in the README tables
- [ ] `CHANGELOG.md` updated under "unreleased"
- [ ] No new runtime dependencies (pyspark stays optional)
