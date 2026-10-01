"""Phoenix exporter setup with manual, metadata-only spans."""

import os
from functools import lru_cache


def configure_phoenix_tracing() -> None:
    endpoint = os.getenv("PHOENIX_COLLECTOR_ENDPOINT")
    if not endpoint:
        return
    _register(endpoint)


@lru_cache(maxsize=4)
def _register(endpoint: str) -> None:
    from phoenix.otel import register

    # Automatic OpenAI instrumentation may capture prompts and ABAP source.
    # The adapter creates its own metadata-only spans instead.
    register(project_name=os.getenv("PHOENIX_PROJECT_NAME", "fabric-migration"),
             endpoint=endpoint, auto_instrument=False, batch=True, verbose=False)
