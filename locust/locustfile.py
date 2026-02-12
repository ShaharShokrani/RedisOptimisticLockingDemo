import os
import random
from locust import HttpUser, task, between

BASE_USER_ID = os.getenv("USER_ID", "123")
AMOUNT = int(os.getenv("WITHDRAW_AMOUNT", "3"))
AMOUNT_JITTER = int(os.getenv("AMOUNT_JITTER", "0"))


class RedisCompareUser(HttpUser):
    wait_time = between(0.0, 0.02)

    def on_start(self):
        init_balance = os.getenv("INIT_BALANCE")
        if init_balance:
            self.client.post(
                "/init",
                params={"userId": BASE_USER_ID, "balance": init_balance},
                name="/init"
            )

    def _hit(self, path, name):
        amt = AMOUNT
        if AMOUNT_JITTER > 0:
            amt = max(1, amt + random.randint(-AMOUNT_JITTER, AMOUNT_JITTER))

        with self.client.post(
                path,
                params={"userId": BASE_USER_ID, "amount": amt},
                name=name,
                catch_response=True,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"HTTP {resp.status_code}")
                return

            data = resp.json()
            status = data.get("status", "unknown")

            resp.success()

    @task(5)
    def withdraw_watch(self):
        self._hit("/withdraw_watch", "withdraw_watch")

    @task(5)
    def withdraw_lock_lua(self):
        self._hit("/withdraw_lock_lua", "withdraw_lock_lua")

    @task(5)
    def withdraw_medallion_distributed_lock(self):
        self._hit("/withdraw_medallion_distributed_lock", "withdraw_medallion_distributed_lock")

    @task(1)
    def check_balance(self):
        with self.client.get(
                "/balance",
                params={"userId": BASE_USER_ID},
                name="/balance",
                catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            elif resp.status_code == 404:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(1)
    def health_check(self):
        with self.client.get(
                "/health",
                name="/health",
                catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")
