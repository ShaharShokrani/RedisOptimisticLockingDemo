using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;


static string BalanceKey(string prefix, string userId) => $"{prefix}:balance:{userId}";
static string LockKey(string prefix, string userId) => $"{prefix}:lock:balance:{userId}";

var builder = WebApplication.CreateBuilder(args);

// Config via env vars (nice for docker / CI)
var redisConn = Environment.GetEnvironmentVariable("REDIS_CONN") ?? "localhost:6379";
var appPrefix = Environment.GetEnvironmentVariable("KEY_PREFIX") ?? "demo";
var maxRetries = int.TryParse(Environment.GetEnvironmentVariable("MAX_RETRIES"), out var r) ? r : 20;

// Optimized configuration for high throughput (10k+ ops/sec)
var config = ConfigurationOptions.Parse(redisConn);
config.AsyncTimeout = 5000; // 5 seconds for async operations
config.ConnectTimeout = 5000; // 5 seconds to connect
config.SyncTimeout = 5000; // 5 seconds for sync operations
config.AbortOnConnectFail = false; // Keep retrying on connection failures
config.AllowAdmin = false;
// config.PreserveAsyncOrder = false; // Obsolete in newer versions - removed for compatibility
config.ConnectRetry = 3;
config.ReconnectRetryPolicy = new ExponentialRetry(100, 1000); // Exponential backoff

builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(config));

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { ok = true }));

// ============================================================================
// INITIALIZATION ENDPOINT
// ============================================================================
// Initialize balance key for testing
// Example: /init?userId=123&balance=100000
app.MapPost("/init", async (
    [FromServices] IConnectionMultiplexer mux,
    [FromQuery] string userId,
    [FromQuery] long balance) =>
{
    var db = mux.GetDatabase();
    var key = BalanceKey(appPrefix, userId);
    await db.StringSetAsync(key, balance);
    return Results.Ok(new { userId, balance });
});

// Read balance (optional but useful for checking)
app.MapGet("/balance", async (
    [FromServices] IConnectionMultiplexer mux,
    [FromQuery] string userId) =>
{
    var db = mux.GetDatabase();
    var key = BalanceKey(appPrefix, userId);
    var v = await db.StringGetAsync(key);
    if (!v.HasValue) return Results.NotFound();
    return Results.Ok(new { userId, balance = v.ToString() });
});

// ============================================================================
// LOCKING STRATEGY 1: Optimistic Locking (Transaction-based)
// ============================================================================
// Approach: Uses Redis transactions with conditional execution (WATCH-like semantics)
// 
// How it works:
//   1. Read current balance
//   2. Create transaction with condition: "only execute if value unchanged"
//   3. Queue decrement operation
//   4. Execute transaction (fails if another process modified the value)
//   5. Retry on conflict (up to maxRetries times)
//
// Characteristics:
//   - No explicit lock key needed
//   - Retries on transaction conflicts
//   - Exponential backoff reduces contention
//   - Best for: Low to medium contention scenarios
//
// Example: /withdraw_watch?userId=123&amount=3
app.MapPost("/withdraw_watch", async (
    [FromServices] IConnectionMultiplexer mux,
    [FromQuery] string userId,
    [FromQuery] long amount) =>
{
    if (amount <= 0) return Results.BadRequest(new { error = "amount must be > 0" });

    var db = mux.GetDatabase();
    var key = BalanceKey(appPrefix, userId);

    int attempt = 0;
    while (attempt < maxRetries)
    {
        attempt++;

        // STEP 1: Read current balance
        RedisValue currentVal = await db.StringGetAsync(key);
        if (!currentVal.HasValue)
            return Results.NotFound(new { status = "missing" });

        if (!long.TryParse(currentVal.ToString(), out var currentBalance))
            return Results.Problem("balance parse error");

        if (currentBalance < amount)
            return Results.Ok(new { status = "insufficient", attempt });

        // STEP 2: Create transaction with conditional execution
        // Condition: Only execute if the key value hasn't changed since we read it
        var tran = db.CreateTransaction();
        tran.AddCondition(Condition.StringEqual(key, currentVal));

        // STEP 3: Queue the decrement operation
        var newBalTask = tran.StringDecrementAsync(key, amount);

        // STEP 4: Execute transaction (atomic check-and-modify)
        // Returns false if condition failed (another process modified the value)
        bool executed = await tran.ExecuteAsync();

        if (executed)
        {
            // Success: transaction executed atomically
            long newBalance = await newBalTask;
            return Results.Ok(new { status = "ok", attempt, newBalance });
        }

        // STEP 5: Conflict detected - retry with exponential backoff
        // Backoff reduces stampede effect under high contention
        await Task.Delay(Math.Min(5 * attempt, 50));
    }

    // Retries exhausted
    return Results.Ok(new { status = "conflict", attempt = maxRetries });
});

// ============================================================================
// LOCKING STRATEGY 2: Pure Lua Script (No Lock - Atomic Operation)
// ============================================================================
// Approach: Single atomic Lua script execution - all logic happens server-side
// 
// How it works:
//   1. Send Lua script to Redis with key and amount
//   2. Redis executes script atomically (single operation)
//   3. Script handles: read balance, check, decrement - all in one atomic step
//
// Characteristics:
//   - No lock key needed (script is atomic)
//   - Single round-trip to Redis (best performance)
//   - No retry logic needed (no conflicts possible)
//   - Best for: High contention, simple operations
//
// Script return values:
//   -1 = key not found
//    0 = insufficient balance
//   >0 = new balance after withdrawal
//
// Example: /withdraw_lock_lua?userId=123&amount=3
app.MapPost("/withdraw_lock_lua", async (
    [FromServices] IConnectionMultiplexer mux,
    [FromQuery] string userId,
    [FromQuery] long amount) =>
{
    if (amount <= 0) return Results.BadRequest(new { error = "amount must be > 0" });

    var db = mux.GetDatabase();
    var key = BalanceKey(appPrefix, userId);

    // Execute Lua script atomically (all logic in one Redis operation)
    // Redis caches scripts by SHA for better performance
    var res = (long)await db.ScriptEvaluateAsync(
        RedisScripts.WithdrawScript, 
        new RedisKey[] { key }, 
        new RedisValue[] { amount }
    );

    if (res == -1) return Results.NotFound(new { status = "missing" });
    if (res == 0)  return Results.Ok(new { status = "insufficient" });

    return Results.Ok(new { status = "ok", newBalance = res });
});

// ============================================================================
// LOCKING STRATEGY 3: Safe Token-Based Lock (Token Verification)
// ============================================================================
// Approach: Token-based lock with SAFE deletion using Lua script token verification
// 
// How it works:
//   1. Acquire lock with unique token (GUID)
//   2. Execute critical section
//   3. Release lock using Lua script that verifies token matches before deleting
//
// SAFETY MECHANISM:
//   - Process A acquires lock with token T1, TTL = 800ms
//   - Process A's critical section takes longer than 800ms
//   - Lock expires, Process B acquires lock with token T2
//   - Process A finishes and tries to release lock
//   - Lua script checks: lock value == T1? NO (it's T2 now)
//   - Script returns 0 (doesn't delete) - Process B's lock is safe!
//
// Characteristics:
//   - Explicit lock key with token verification
//   - Parameters: 800ms TTL, 250ms wait budget, 10ms retry delay
//   - SAFE deletion: Lua script verifies token before deleting
//   - Best for: Production-safe lock implementation
//
// Example: /withdraw_polling_token?userId=123&amount=3
app.MapPost("/withdraw_custom_token_lock", async (
    [FromServices] IConnectionMultiplexer mux,
    [FromQuery] string userId,
    [FromQuery] long amount) =>
{
    if (amount <= 0) return Results.BadRequest(new { error = "amount must be > 0" });

    var db = mux.GetDatabase();
    var balKey = BalanceKey(appPrefix, userId);
    var lockKey = LockKey(appPrefix, userId);

    // Lock configuration (same as withdraw_polling for fair comparison)
    var lockTtl = TimeSpan.FromMilliseconds(800);   // Lock expiration time
    var waitBudget = TimeSpan.FromMilliseconds(250); // Max time to wait for lock
    var retryDelay = TimeSpan.FromMilliseconds(10); // Delay between retry attempts

    try
    {
        // STEP 1: Acquire lock using SafeRedisLock class
        // LockAsync handles polling, token generation, and timeout internally
        // await using ensures automatic lock release via DisposeAsync()
        // SAFE: Release uses Lua script with token verification
        await using (await new SafeRedisLock(db).LockAsync(lockKey, lockTtl, waitBudget, retryDelay))
        {
            // STEP 2: Critical section (protected by lock)
            // Lock ensures only one process executes this section at a time
            // SAFE: If TTL expires and another process acquires lock, our release won't delete their lock
            // because Lua script verifies token matches before deleting
            var currentVal = await db.StringGetAsync(balKey);
            if (!currentVal.HasValue)
                return Results.NotFound(new { status = "missing" });

            if (!long.TryParse(currentVal.ToString(), out var currentBalance))
                return Results.Problem("balance parse error");

            if (currentBalance < amount)
                return Results.Ok(new { status = "insufficient" });

            var newBalance = await db.StringDecrementAsync(balKey, amount);
            return Results.Ok(new { status = "ok", newBalance });
        }
    }
    catch (Exception ex) when (ex.Message.Contains("timeout"))
    {
        // Lock acquisition timeout - convert exception to status response
        return Results.Ok(new { status = "lock_busy", error = ex.Message });
    }
});

// ============================================================================
// LOCKING STRATEGY 4: RedisLock Class (Production-style UNSAFE Implementation)
// ============================================================================
// Approach: Matches production code style - uses direct DEL without token verification
// 
// How it works:
//   1. Acquire lock with empty string value (no token)
//   2. Execute critical section
//   3. Release lock using direct DEL (NO token verification)
//
// RACE CONDITION SCENARIO:
//   - Process A acquires lock, TTL = 800ms
//   - Process A's critical section takes longer than 800ms
//   - Lock expires, Process B acquires lock
//   - Process A finishes and calls DEL (no token check)
//   - Process A deletes Process B's lock! (WRONG PROCESS DELETES LOCK)
//
// Characteristics:
//   - No token used (empty string as lock value)
//   - Parameters: 800ms TTL, 250ms wait budget, 10ms retry delay (SAME as Strategy 3)
//   - UNSAFE deletion demonstrates race condition
//   - Best for: Comparing safe vs unsafe lock release (only difference is token verification)
//
// Example: /withdraw_polling?userId=123&amount=3
app.MapPost("/withdraw_custom_lock", async (
    [FromServices] IConnectionMultiplexer mux,
    [FromQuery] string userId,
    [FromQuery] long amount) =>
{
    if (amount <= 0) return Results.BadRequest(new { error = "amount must be > 0" });

    var db = mux.GetDatabase();
    var balKey = BalanceKey(appPrefix, userId);
    var lockKey = LockKey(appPrefix, userId);

    // Lock configuration (EXACTLY same as withdraw_polling_token for fair comparison)
    var lockTtl = TimeSpan.FromMilliseconds(800);   // Lock expiration time
    var waitBudget = TimeSpan.FromMilliseconds(250); // Max time to wait for lock
    var retryDelay = TimeSpan.FromMilliseconds(10); // Delay between retry attempts

    try
    {
        // STEP 1: Acquire lock using RedisLock class (production-style, UNSAFE)
        // LockAsync handles polling and timeout internally
        // await using ensures automatic lock release via DisposeAsync()
        // WARNING: Release uses direct DEL without token verification (UNSAFE)
        await using (await new RedisLock(db).LockAsync(lockKey, lockTtl, waitBudget, retryDelay))
        {
            // STEP 2: Critical section (protected by lock)
            // Lock ensures only one process executes this section at a time
            var currentVal = await db.StringGetAsync(balKey);
            if (!currentVal.HasValue)
                return Results.NotFound(new { status = "missing" });

            if (!long.TryParse(currentVal.ToString(), out var currentBalance))
                return Results.Problem("balance parse error");

            if (currentBalance < amount)
                return Results.Ok(new { status = "insufficient" });

            var newBalance = await db.StringDecrementAsync(balKey, amount);
            return Results.Ok(new { status = "ok", newBalance });
        }
    }
    catch (Exception ex) when (ex.Message.Contains("timeout"))
    {
        // Lock acquisition timeout - convert exception to status response
        // (matches withdraw_lock_token behavior for consistency)
        return Results.Ok(new { status = "lock_busy", error = ex.Message });
    }
});

// ============================================================================
// LOCKING STRATEGY 5: RedisDistributedLock (TryAcquire Pattern)
// ============================================================================
// Approach: TryAcquire pattern - returns null if lock unavailable (non-blocking style)
// - Different API: TryAcquireAsync returns null on failure (no exception)
// - Lock TTL automatically set to timeout + 5 seconds buffer
// - 50ms retry delay (hardcoded in class)
// - Best for: When you prefer null-check over exception handling
// Example: /withdraw_distributed_lock?userId=123&amount=3
app.MapPost("/withdraw_medallion_distributed_lock", async (
    [FromServices] IConnectionMultiplexer mux,
    [FromQuery] string userId,
    [FromQuery] long amount) =>
{
    if (amount <= 0) return Results.BadRequest(new { error = "amount must be > 0" });

    var db = mux.GetDatabase();
    var balKey = BalanceKey(appPrefix, userId);
    var lockKey = LockKey(appPrefix, userId);
    const int REDIS_LOCK_TIMEOUT = 5; // 5 second timeout

    // STEP 1: Try to acquire lock (returns null if unavailable)
    // Note: Lock TTL is automatically set to timeout + 5 seconds
    var lockHandle = await new Medallion.Threading.Redis.RedisDistributedLock(lockKey, db)
        .TryAcquireAsync(TimeSpan.FromSeconds(REDIS_LOCK_TIMEOUT));

    if (lockHandle == null)
    {
        return Results.Ok(new { status = "lock_busy", message = "Could not acquire lock" });
    }

    try
    {
        // STEP 2: Critical section (protected by lock)
        // await using ensures automatic lock release via DisposeAsync()
        await using (lockHandle)
        {
            // Lock ensures only one process executes this section at a time
            var currentVal = await db.StringGetAsync(balKey);
            if (!currentVal.HasValue)
                return Results.NotFound(new { status = "missing" });

            if (!long.TryParse(currentVal.ToString(), out var currentBalance))
                return Results.Problem("balance parse error");

            if (currentBalance < amount)
                return Results.Ok(new { status = "insufficient" });

            var newBalance = await db.StringDecrementAsync(balKey, amount);
            return Results.Ok(new { status = "ok", newBalance });
        }
    }
    catch (Exception ex)
    {
        return Results.Problem($"Error during withdrawal: {ex.Message}");
    }
});

app.Run();