"""HTTP test helpers; no inference implementation is imported."""


def register(client, name="alice", password="correct-horse-battery"):
    response = client.post("/api/v1/auth/register", json={"login_id": name, "password": password})
    assert response.status_code == 201, response.text
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]
    return response
