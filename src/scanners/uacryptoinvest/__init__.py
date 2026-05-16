"""UACryptoInvest scanner adapter.

This package is intentionally self-contained. The product only depends on the
public ``Scanner`` interface exposed by ``source.UACryptoInvestScanner``.
"""

from .source import UACryptoInvestScanner

__all__ = ["UACryptoInvestScanner"]