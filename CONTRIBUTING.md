# Contributing

Use Python 3.11 or newer and an isolated virtual environment. Install the checkout
with `python -m pip install -e ".[dev]"` from that environment, then run
`python -m pytest -q`. Set `MEMKERNEL_TOKENIZER=heuristic` for offline tests.
The live model test is opt-in and may incur API charges; the normal suite does not
require model credentials.

Reproduce bugs with a failing regression test before fixing them. Keep public API,
CLI, MCP schemas, callers, examples, and documentation consistent. Preserve existing
events, rollback semantics, lease fencing, and read-only protection. Avoid adding
dependencies for behavior the existing code or standard library can provide.

Use synthetic data. Never commit a real memory database, signing key, sync policy,
client configuration, model transcript, or credentials. Security reports belong in
the private process described in [SECURITY.md](SECURITY.md).

CI checks syntax and imports, Linux, macOS, Windows, Python 3.11/3.14, both supported MCP SDK generations,
examples, dependencies against current advisories, and installation of a clean wheel.
The publish workflow reruns CI and checks that the release tag matches the package
version before publishing through PyPI Trusted Publishing.
