import os
import random
from locust import HttpUser, task, between, events

BASE_USER_ID = os.getenv("USER_ID", "123")
AMOUNT = int(os.getenv("WITHDRAW_AMOUNT", "3"))
AMOUNT_JITTER = int(os.getenv("AMOUNT_JITTER", "0"))


class RedisCompareUser(HttpUser):
    wait_time = between(0.0, 0.02)

    def on_start(self):
        """
        Runs ONCE per user.
        Initializes balance so the test is repeatable.
        """
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

            # Optional: Track business logic failures separately
            # You can uncomment this if you want to see breakdowns like:
            # - withdraw_watch_ok vs withdraw_watch_insufficient vs withdraw_watch_lock_busy
            # events.request.fire(
            #     request_type="APP",
            #     name=f"{name}_{status}",
            #     response_time=0,
            #     response_length=0,
            #     exception=None,
            # )

            # Mark HTTP request as successful (HTTP 200)
            resp.success()

    # ---- TASKS (these run repeatedly) ----

    @task(5)
    def withdraw_watch(self):
        self._hit("/withdraw_watch", "withdraw_watch")

    @task(5)
    def withdraw_lock_lua(self):
        self._hit("/withdraw_lock_lua", "withdraw_lock_lua")

    @task(5)
    def withdraw_custom_token_lock(self):
        self._hit("/withdraw_custom_token_lock", "withdraw_custom_token_lock")

    @task(5)
    def withdraw_custom_lock(self):
        self._hit("/withdraw_custom_lock", "withdraw_custom_lock")

    @task(5)
    def withdraw_medallion_distributed_lock(self):
        self._hit("/withdraw_medallion_distributed_lock", "withdraw_medallion_distributed_lock")

    @task(1)
    def check_balance(self):
        """Optional: Check balance endpoint (lower weight since it's read-only)"""
        with self.client.get(
                "/balance",
                params={"userId": BASE_USER_ID},
                name="/balance",
                catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            elif resp.status_code == 404:
                resp.success()  # Not found is acceptable
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(1)
    def health_check(self):
        """Optional: Health check endpoint (lower weight)"""
        with self.client.get(
                "/health",
                name="/health",
                catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")