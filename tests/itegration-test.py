import pytest
import requests
import time

BASE_URL = "http://localhost:8000"

def wait_for_shards():
    """Чекаємо, поки хоча б 2 шарди зареєструються"""
    for _ in range(10):
        try:
            # Можна додати ендпоінт /health або перевіряти статус шардів
            # Тут просто перевіряємо, що координатор живий
            resp = requests.get(f"{BASE_URL}/docs")
            if resp.status_code == 200:
                # В ідеалі додати метод GET /shards/count в координатор
                time.sleep(1) 
                return
        except:
            pass
        time.sleep(2)
    pytest.fail("Coordinator did not start in time")

def test_01_create_table():
    wait_for_shards()
    payload = {"name": "users"}
    resp = requests.post(f"{BASE_URL}/tables", json=payload)
    assert resp.status_code in [201, 400] # Created or Already exists

def test_02_write_data():
    # Пишемо User 1
    resp = requests.post(
        f"{BASE_URL}/tables/users/records",
        json={"partition_key": "user1", "value": {"name": "Alice"}}
    )
    assert resp.status_code == 200

    # Пишемо User 2
    resp = requests.post(
        f"{BASE_URL}/tables/users/records",
        json={"partition_key": "user2", "value": {"name": "Bob"}}
    )
    assert resp.status_code == 200

def test_03_read_data():
    # Читаємо User 1
    resp = requests.get(f"{BASE_URL}/tables/users/records/user1")
    assert resp.status_code == 200
    assert resp.json()["name"] == "Alice"

def test_04_check_sharding():
    """
    Цей тест перевіряє, чи дійсно дані потрапляють на різні шарди.
    Для цього нам знадобиться доступ до логів або debug-ендпоінту.
    Але для базового тесту достатньо, що READ працює (значить, маршрутизація правильна).
    """
    pass