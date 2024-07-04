from fastapi.testclient import TestClient

from pipelines.add_users_to_study.handler import app

client = TestClient(app)


def test_add_user():
    url = "/users"
    payload = {
        "bluesky_handle": "testuser.bsky.social",
        "condition": "reverse_chronological",
        "bluesky_user_did": "did:plc:testuser",
        "is_study_user": True
    }
    response = client.post(url, json=payload)
    assert response.status_code == 201
    assert response.json()["bluesky_handle"] == "testuser.bsky.social"


def test_get_user():
    bluesky_user_did = "did:plc:testuser"
    url = f"/users/{bluesky_user_did}"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json()["bluesky_user_did"] == bluesky_user_did


def test_delete_user():
    bluesky_user_did = "did:plc:testuser"
    url = f"/users/{bluesky_user_did}"
    response = client.delete(url)
    assert response.status_code == 200
    assert response.json()["message"] == "Operation successful. User deleted successfully."  # noqa


if __name__ == "__main__":
    test_add_user()
    test_get_user()
    test_delete_user()
    print("All tests passed!")
