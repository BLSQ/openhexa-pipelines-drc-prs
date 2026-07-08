# AGENTS.md

## Repository purpose

This repository holds [OpenHEXA](https://github.com/BLSQ/openhexa) pipeline
templates used by the DRC PRS project, maintained by Bluesquare's Data
Services team. Every pipeline extracts, transforms, and pushes data between
DHIS2 instances (e.g. syncing org units/datasets, computing CMM or
exhaustivity indicators) using the `openhexa.sdk` and `openhexa.toolbox`
libraries.

## Repository layout

Each top-level directory is an **independent, self-contained OpenHEXA
pipeline** — there is no shared package between them:

- `dhis2_cmm_morbidity/` — syncs org units and pushes CMM morbidity indicators.
- `dhis2_cmm_push/` — computes and pushes CMM (rolling average) data elements.
- `dhis2_dataset_sync/` — syncs org units, datasets, and data elements between DHIS2 instances.
- `dhis2_dataset_sync_completions/` — syncs dataset completion statuses.
- `dhis2_exhaustivity/` — computes and pushes exhaustivity indicators.

Within each pipeline directory:

- `pipeline.py` — the OpenHEXA entry point, decorated with `@pipeline` and
  `@parameter` from `openhexa.sdk`; tasks report progress via `current_run`.
- `utils.py` — pipeline-specific helpers (config loading, DHIS2 connection,
  date/period handling, parquet I/O, etc.).
- `d2d_library/` — helper modules (DHIS2 extraction, pushing, org unit
  alignment, queueing). **This code is copied independently into each
  pipeline and has diverged between them** — a fix in one pipeline's
  `d2d_library` does not automatically apply to another's; check each copy.
- `workspace.yaml` — local OpenHEXA workspace config (DB + DHIS2 connections)
  used for local runs via the OpenHEXA CLI. Treat any credentials in this
  file as sensitive — never introduce or commit real ones.
- `requirements.txt` — pinned `openhexa.toolbox` / `openhexa.sdk` versions
  for that pipeline (versions vary between pipelines, upgrade deliberately).
- `readme.md` — pipeline-specific overview of what it does (most pipelines
  have one; read it before modifying that pipeline).

## Deployment

Pipelines are pushed to the OpenHEXA workspace `drc-prs`. Only
`dhis2_cmm_morbidity` currently has an automated GitHub Actions workflow
(`.github/workflows/push-pipeline.yml`, using `blsq/openhexa-push-pipeline-action`,
triggered on push to `main`). Other pipelines are deployed manually via the
OpenHEXA CLI — do not assume a merge to `main` auto-deploys them.

## Testing

There is no automated test suite in this repository. Validate changes by
running the pipeline locally against the `workspace.yaml` connections (or a
DHIS2 demo instance) and by reading through the affected `d2d_library`
modules, since there's no CI to catch regressions.

## Code Style

- Line length: 120 characters (ruff enforced).
- Python 3.12+.
- Ruff rules enabled: pyflakes, pycodestyle, isort, pydocstyle, pyupgrade,
  flake8-annotations, bugbear, and others (see `pyproject.toml`).
- **Docstrings:** Google-style (`tool.ruff.lint.pydocstyle` convention is
  `google`). Every function/method docstring follows a standard structure —
  a brief one-line description, an `Args:` section briefly describing each
  parameter, and a `Returns:` section if the function returns a value.

Run `ruff check .` and `ruff format .` from the repo root before committing.
