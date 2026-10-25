# Changelog

## 0.1.0 (unreleased)

First release.

* Parser for Spark physical plans (simple, extended and formatted explain modes; AQE final/initial plans; scan details).
* `expect()` fluent plan assertions (shuffles, join strategies, broadcasts, pushdown, partition pruning, column pruning, Python UDFs, scans, generic operators).
* Plan lock files with normalization, three compare modes, semantic cost diff, `PLANLOCK_UPDATE` and CI-aware behaviour.
* pytest plugin: `planlock` fixture, `--planlock-update`, `--planlock-mode`, `--planlock-fail-on`, run summary.
* Plan linter with rules PL001 to PL009.
* `planlock` CLI: `lint`, `summary`, `diff`, `tree`, `normalize`.
