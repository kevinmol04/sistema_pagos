from fastapi.testclient import TestClient
from app import app

client = TestClient(app)

def _base_body():
    return {
        "transaction_id": 1,
        "amount_mxn": 100.0,
        "customer_txn_30d": 0,
        "geo_state": "NL",
        "device_type": "mobile",
        "chargeback_count": 0,
        "hour": 12,
        "product_type": "digital",
        "latency_ms": 50,
        "user_reputation": "new",
        "device_fingerprint_risk": "low",
        "ip_risk": "low",
        "email_risk": "low",
        "bin_country": "MX",
        "ip_country": "MX",
    }

def test_transaction_accepted_low_risk():
    """Cubre la rama ACCEPTED (todo bajo riesgo, sin penalizaciones)."""
    body = _base_body()
    r = client.post("/transaction", json=body)
    assert r.status_code == 200
    data = r.json()
    assert data["decision"] == "ACCEPTED"

def test_geo_mismatch_adds_points():
    """Cubre la rama de geo mismatch (bin_country != ip_country)."""
    body = _base_body()
    body["bin_country"] = "MX"
    body["ip_country"] = "US"
    r = client.post("/transaction", json=body)
    assert r.status_code == 200
    data = r.json()
    assert data["risk_score"] >= 1
    assert "decision" in data

def test_latency_extreme_branch():
    """Cubre la rama de latencia extrema."""
    body = _base_body()
    body["latency_ms"] = 999_999  # valor muy alto para disparar la regla
    r = client.post("/transaction", json=body)
    assert r.status_code == 200
    data = r.json()
    assert data["risk_score"] >= 1

def test_frequency_buffer_for_trusted():
    """Cubre la rama del buffer de frecuencia para trusted/recurrent."""
    body = _base_body()
    body["user_reputation"] = "trusted"
    body["customer_txn_30d"] = 5   # suficiente para activar el buffer
    # Forzamos que haya algún punto previo para que se pueda restar
    body["ip_country"] = "US"      # mismatch sencillo
    r = client.post("/transaction", json=body)
    assert r.status_code == 200
    data = r.json()
    assert "decision" in data

def test_night_hour_adds_points():
    """Cubre la rama de noche (is_night(hour) == True)."""
    body = _base_body()
    body["hour"] = 2  # hora típica nocturna
    r = client.post("/transaction", json=body)
    assert r.status_code == 200
    data = r.json()
    assert data["risk_score"] >= 1
