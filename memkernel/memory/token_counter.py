"""Context-window budget estimator.

Uses ``tiktoken`` (``cl100k_base``) when its encoding can be loaded; the
first load may need network access to fetch the BPE file. When it cannot be
loaded, or when ``backend="heuristic"`` (or ``MEMKERNEL_TOKENIZER=heuristic``)
is requested, a regex estimator is used instead. The heuristic is deliberately
conservative (it over-counts relative to BPE tokenizers), which is the safe
direction for a budget.
"""

from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from typing import Any

log = logging.getLogger(__name__)

# Letter runs, digit runs of <= 3 (as cl100k splits numbers), or any single
# other non-space character.
_TOKEN_RE = re.compile(r"[^\W\d_]+|\d{1,3}|[^\s]", re.UNICODE)


def heuristic_count(text: str) -> int:
    total = 0
    for match in _TOKEN_RE.finditer(text):
        piece = match.group(0)
        total += (len(piece) + 3) // 4 if piece[0].isalpha() else 1
    return total


@lru_cache(maxsize=4)
def _load_encoding(name: str) -> Any:
    import tiktoken

    return tiktoken.get_encoding(name)


class TokenCounter:
    """``count(text) -> int``. ``backend`` is ``"auto"``, ``"tiktoken"`` or
    ``"heuristic"``; ``backend_name`` reports what is actually in use."""

    def __init__(self, backend: str | None = None, encoding: str = "cl100k_base") -> None:
        backend = (backend or os.environ.get("MEMKERNEL_TOKENIZER") or "auto").lower()
        if backend not in {"auto", "tiktoken", "heuristic"}:
            raise ValueError(f"unknown tokenizer backend {backend!r}")
        self._encoding = None
        self.backend_name = "heuristic"
        if backend in {"auto", "tiktoken"}:
            try:
                self._encoding = _load_encoding(encoding)
                self.backend_name = f"tiktoken:{encoding}"
            except Exception as exc:  # ImportError, network failure, corrupt cache...
                if backend == "tiktoken":
                    raise RuntimeError(f"tiktoken encoding {encoding!r} unavailable: {exc}") from exc
                log.warning("tiktoken unavailable (%s); using heuristic token estimator", exc)

    def count(self, text: str) -> int:
        if not text:
            return 0
        if self._encoding is not None:
            # disallowed_special=() treats "<|endoftext|>" etc. in untrusted
            # content as plain text instead of raising.
            return len(self._encoding.encode(text, disallowed_special=()))
        return heuristic_count(text)
