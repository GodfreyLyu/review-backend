import time


class FakeReviewClient:
    simulated = True

    def __init__(self, result="## Summary\nCheck empty input before division.", delay=0.01):
        self.result, self.delay = result, delay
        self.calls = self.active = self.peak = 0
        self.connections = 0
        self.closed = False

    def connect(self):
        self.connections += 1

    def close(self):
        self.closed = True

    def review(self, source, language, stop):
        self.calls += 1
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            stop.wait(self.delay)
            if isinstance(self.result, Exception):
                raise self.result
            return self.result
        finally:
            self.active -= 1


def wait_ready(client):
    for _ in range(200):
        if client.get("/health/ready").status_code == 200:
            return
        time.sleep(0.01)
    raise AssertionError("Application did not become ready")


def wait_review(client, review_id):
    for _ in range(200):
        response = client.get(f"/api/v1/reviews/{review_id}")
        assert response.status_code == 200, response.text
        if response.json()["status"] in ("completed", "failed"):
            return response.json()
        time.sleep(0.01)
    raise AssertionError("Review did not finish")


def register(client, name="alice", password="correct-horse-battery"):
    response = client.post("/api/v1/auth/register", json={"login_id": name, "password": password})
    assert response.status_code == 201, response.text
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]
    return response
