"""The live-agent harness must never publish home paths or credentials."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
import live_agents as la  # noqa: E402


def test_sanitize_strips_home_paths_at_any_escaping_depth_and_secrets(monkeypatch):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: Path("C:/Users/alice")))
    for raw, want in [
        (r"C:\Users\alice\x", r"~\x"),
        (r"C:\\Users\\ALICE\\x", r"~\\x"),
        ("/c/Users/alice/a", "~/a"),
        ("C:/Users/alicex/a", "C:/Users/alicex/a"),  # a different user is left alone
    ]:
        assert la.sanitize(raw) == want
    assert la.sanitize("mail me@example.com key sk-abcdefghijklmnop") == "mail <email> key <redacted>"
