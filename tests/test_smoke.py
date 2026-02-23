import os
from fastapi.testclient import TestClient

os.environ["DB_PATH"] = "/tmp/test.db"
from app.main import app

client = TestClient(app)

def test_status_ok():
  r = client.get("/api/status")
  assert r.status_code == 200