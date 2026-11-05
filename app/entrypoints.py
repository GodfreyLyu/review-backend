"""Independent HTTP process; no inference executor or broker starts here."""

import os

import uvicorn


def api():
    uvicorn.run(
        "app.main:create_app",
        factory=True,
        host=os.getenv("API_HOST", "127.0.0.1"),
        port=int(os.getenv("API_PORT", "8080")),
    )


if __name__ == "__main__":
    api()
