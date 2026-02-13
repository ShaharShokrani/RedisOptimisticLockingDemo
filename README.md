# Redis Optimistic Locking Demo

A minimal ASP.NET Core API that implements **concurrent withdrawal** from a Redis-backed balance using three strategies: **Lua script** (atomic), **distributed lock** (Medallion), and **optimistic locking** (WATCH / condition-based transaction). Includes sequence diagrams and a Locust load-test setup so you can compare latency and behavior.

## What it does

- **Withdrawal**: read balance → check sufficient → deduct amount → append to withdrawal history.
- **Three endpoints** implement the same operation with different concurrency approaches so you can benchmark and compare.

| Endpoint | Strategy | Description |
|----------|----------|-------------|
| `POST /withdraw_lock_lua` | Lua script | Single round-trip; script runs atomically in Redis. |
| `POST /withdraw_medallion_distributed_lock` | Distributed lock | Acquire lock → read/update → release (Medallion). |
| `POST /withdraw_watch` | Optimistic locking | Read; then transaction with `Condition.StringEqual`; retry on conflict. |

## Requirements

- [.NET 9 SDK](https://dotnet.microsoft.com/download)
- [Redis](https://redis.io/) (local or remote)
- Python 3.x + [Locust](https://locust.io/) (optional, for load testing)

## Quick start

1. **Start Redis** (e.g. `redis-server` or Docker: `docker run -p 6379:6379 redis`).

2. **Configure and run the API** (optional env vars: `REDIS_CONN`, `KEY_PREFIX`, `MAX_RETRIES`):

   ```bash
   # default: Redis at localhost:6379, key prefix "demo", max 20 retries for optimistic path
   dotnet run
   ```

   API runs at **http://localhost:5298** (see `Properties/launchSettings.json` for other profiles).

3. **Initialize a balance and try withdrawals**:

   ```bash
   curl -X POST "http://localhost:5298/init?userId=u1&balance=10000"
   curl "http://localhost:5298/balance?userId=u1"
   curl -X POST "http://localhost:5298/withdraw_watch?userId=u1&amount=50"
   curl -X POST "http://localhost:5298/withdraw_lock_lua?userId=u1&amount=50"
   curl -X POST "http://localhost:5298/withdraw_medallion_distributed_lock?userId=u1&amount=50"
   ```

## API

| Method | Path | Query | Description |
|--------|------|-------|-------------|
| GET | `/health` | — | Health check. |
| POST | `/init` | `userId`, `balance` | Set initial balance for a user. |
| GET | `/balance` | `userId` | Get current balance (404 if missing). |
| POST | `/withdraw_watch` | `userId`, `amount` | Withdraw via optimistic locking (retries on conflict). |
| POST | `/withdraw_lock_lua` | `userId`, `amount` | Withdraw via Lua script (atomic). |
| POST | `/withdraw_medallion_distributed_lock` | `userId`, `amount` | Withdraw under Medallion distributed lock. |

Withdrawal responses include a `status` field: `ok`, `insufficient`, `missing`, `lock_busy` (Medallion only), or `conflict` (optimistic after max retries).

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_CONN` | `localhost:6379` | Redis connection string. |
| `KEY_PREFIX` | `demo` | Prefix for Redis keys (balance, lock, history). |
| `MAX_RETRIES` | `20` | Max retries for optimistic locking before returning `conflict`. |

## Load testing with Locust

From the `locust` directory (or with `locust` on `PATH` and `locustfile.py` in the current directory):

```bash
cd locust
pip install locust
locust -f locustfile.py --host=http://localhost:5298
```

Optional env vars for the Locust run:

- `INIT_BALANCE` — if set, each user calls `/init` with this balance before tasks.
- `USER_ID` — default `123` (single user to stress concurrent withdrawals).
- `WITHDRAW_AMOUNT` — default `3`.
- `AMOUNT_JITTER` — optional; adds random variation to amount.

Example with initial balance:

```bash
INIT_BALANCE=100000 locust -f locustfile.py --host=http://localhost:5298
```

Open the URL Locust prints (usually http://localhost:8089), set user count and spawn rate, then run. Compare request stats per endpoint (e.g. `withdraw_watch`, `withdraw_lock_lua`, `withdraw_medallion_distributed_lock`).

## Project layout

```
├── Program.cs              # API and three withdrawal implementations
├── RedisScripts.cs         # Lua script for atomic withdrawal
├── locust/
│   └── locustfile.py       # Locust load test (withdraw + balance + health)
├── docs/
│   ├── *.puml              # PlantUML sequence diagrams
│   ├── *.png               # Rendered diagrams
│   └── medium-article-optimistic-locking.md  # Article draft
└── README.md
```

## Docs and article

- **Sequence diagrams** (PlantUML): `docs/withdraw-flow.puml`, `docs/withdraw-lua.puml`, `docs/withdraw-lua-medallion.puml`; rendered PNGs in `docs/`.
- **Medium-style article** (with benchmarks and code snippets): `docs/medium-article-optimistic-locking.md`.

## License

Use and adapt as you like (no license file in repo).
