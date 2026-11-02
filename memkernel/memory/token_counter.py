"""Context-window budget estimator.

Uses ``tiktoken`` (``cl100k_base``) when its encoding can be loaded; the
first load may need network access to fetch the BPE file. When it cannot be
loaded, or when ``backend="heuristic"`` (or ``MEMKERNEL_TOKENIZER=heuristic``)
is requested, UTF-8 byte length is used as an upper bound for byte-level BPE
tokens. This deliberately over-counts, including multilingual text and
whitespace, so an offline device does not silently under-budget context.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any

log = logging.getLogger(__name__)

def heuristic_count(text: str) -> int:
    return len(text.encode("utf-8"))


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
