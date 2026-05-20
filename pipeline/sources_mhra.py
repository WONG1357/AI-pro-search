"""Deprecated MHRA adapter.

This module is retained only for backward compatibility with older imports.
The project now uses FDA MAUDE only, and this module is intentionally unused.
"""

from __future__ import annotations

from typing import Any


def load_mhra_from_csv(*args: Any, **kwargs: Any) -> None:
    """Deprecated stub retained to avoid import errors in older code paths."""
    print("[WARN] pipeline.sources_mhra is deprecated and unused.")
    return None

