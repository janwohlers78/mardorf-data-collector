"""Bounded mirror failover for one unchanged ECMWF request.

A client belongs to one acquisition call, never to global or shared state.
Native cycle, GRIB and spatial validation stay in the existing callers.
"""
from pathlib import Path

MIRRORS = ("azure", "google", "ecmwf")


class MirrorClient:
    def __init__(self, factory, source="azure"):
        if source not in MIRRORS:
            raise ValueError("Unknown ECMWF mirror")
        self.factory = factory
        self.source = source
        self.sources = (source, *(item for item in MIRRORS if item != source))

    def retrieve(self, **request):
        failures = []
        for source in self.sources:
            try:
                client = self.factory(source=source, model="ifs", resol="0p25",
                    maximum_retries=2, retry_after=5, use_server_retry_after=False)
                result = client.retrieve(**request)
            except Exception as exc:
                # A failed partial file must never become the next mirror's input.
                if request.get("target"):
                    Path(request["target"]).unlink(missing_ok=True)
                failures.append((source, type(exc).__name__))
                last_error = exc
                continue
            self.source = source
            return result
        detail = ", ".join(f"{source}:{kind}" for source,kind in failures)
        raise RuntimeError(f"ECMWF mirrors exhausted ({detail})") from last_error
