# AGENTS.md

## Project context

This repository is a lightweight Python workspace for data and AI-related workflows, with AWS credentials and environment-specific configuration handled locally.

- Dependency manifest: [requirements.txt](requirements.txt)
- Secret and local-environment rules: [.gitignore](.gitignore)
- Keep secrets in `.env` or the environment; do not hardcode AWS keys, tokens, or access credentials in code.
- Avoid committing large datasets, exported CSVs, or generated artifacts unless the repo explicitly requires them.

## Working conventions

- Prefer small, explicit Python scripts over broad framework setup.
- Keep dependencies minimal; if a library is added, update [requirements.txt](requirements.txt) and explain why it is needed.
- Use `python-dotenv` for local config when a script depends on `.env` values.
- Do not expose credentials in logs, debug output, or final summaries.
- Favor readable, maintainable code and avoid unnecessary refactors.

## Validation

Because this repo is small and does not currently appear to have a formal test suite, validate changed Python files with a lightweight smoke check such as:

- `python -m py_compile path/to/changed_file.py`
- or run the relevant script with a minimal local input to confirm behavior.

## Setup

- Create a virtual environment: `python -m venv .venv`
- Activate it:
  - PowerShell: `.venv\Scripts\Activate.ps1`
  - Bash: `source .venv/bin/activate`
- Install dependencies: `python -m pip install -r requirements.txt`

## Guardrails

- Never commit `.env`, AWS credentials, or any secret-bearing file.
- Keep repository contents focused on source and configuration, not raw data or environment artifacts.
- When making changes, prefer the smallest possible patch that solves the task.
