"""Scanner package.

Public entry point — keep imports flat so callers don't reach into the
file layout (encapsulation).

Usage:

    from src.scanners import build_scanner_service
    service = build_scanner_service(notifier)
    if service is not None:
        await service.start()
"""

from .factory import build_scanner_service
from .service import ScannerService

__all__ = ["build_scanner_service", "ScannerService"]
