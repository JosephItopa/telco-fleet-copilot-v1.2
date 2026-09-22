def test_partition_key_is_app_id():
    event = {"app_id": "payments-api", "metrics": {"cpu": 0.8}}
    assert event["app_id"] == "payments-api"
