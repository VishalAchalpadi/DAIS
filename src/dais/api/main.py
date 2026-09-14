"""Entrypoint for running the DAIS API standalone: `python -m dais.api.main`.

Reads DAIS_API_KEY from the environment for auth (see create_app) and
DAIS_API_PORT to override the default port.
"""
import os

import uvicorn

from dais.api.app import create_app

app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DAIS_API_PORT", "8000")))
