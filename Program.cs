using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;


static string BalanceKey(string prefix, string userId) => $"{prefix}:balance:{userId}";
static string LockKey(string prefix, string userId) => $"{prefix}:lock:balance:{userId}";
static string WithdrawalHistoryKey(string prefix, string userId) => $"{prefix}:withdrawals:{userId}";

var builder = WebApplication.CreateBuilder(args);

var redisConn = Environment.GetEnvironmentVariable("REDIS_CONN") ?? "localhost:6379";
var appPrefix = Environment.GetEnvironmentVariable("KEY_PREFIX") ?? "demo";
var maxRetries = int.TryParse(Environment.GetEnvironmentVariable("MAX_RETRIES"), out var r) ? r : 20;

var config = ConfigurationOptions.Parse(redisConn);
config.AsyncTimeout = 5000;
config.ConnectTimeout = 5000;
config.SyncTimeout = 5000;
config.AbortOnConnectFail = false;
config.AllowAdmin = false;
config.ConnectRetry = 3;
config.ReconnectRetryPolicy = new ExponentialRetry(100, 1000);

builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(config));

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { ok = true }));

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

app.MapPost("/withdraw_watch", async (
    [FromServices] IConnectionMultiplexer mux,
    [FromQuery] string userId,
    [FromQuery] long amount) =>
{
    if (amount <= 0) return Results.BadRequest(new { error = "amount must be > 0" });

    var db = mux.GetDatabase();
    var key = BalanceKey(appPrefix, userId);
    var historyKey = WithdrawalHistoryKey(appPrefix, userId);

    int attempt = 0;
    while (attempt < maxRetries)
    {
        attempt++;

        RedisValue currentVal = await db.StringGetAsync(key);
        if (!currentVal.HasValue)
            return Results.NotFound(new { status = "missing" });

        if (!long.TryParse(currentVal.ToString(), out var currentBalance))
            return Results.Problem("balance parse error");

        if (currentBalance < amount)
            return Results.Ok(new { status = "insufficient", attempt });

        var tran = db.CreateTransaction();
        tran.AddCondition(Condition.StringEqual(key, currentVal));

        var newBalTask = tran.StringDecrementAsync(key, amount);
        var pushTask = tran.ListLeftPushAsync(historyKey, amount);

        bool executed = await tran.ExecuteAsync();

        if (executed)
        {
            long newBalance = await newBalTask;
            await pushTask;
            return Results.Ok(new { status = "ok", attempt, newBalance });
        }

        await Task.Delay(Math.Min(5 * attempt, 50));
    }

    return Results.Ok(new { status = "conflict", attempt = maxRetries });
});

app.MapPost("/withdraw_lock_lua", async (
    [FromServices] IConnectionMultiplexer mux,
    [FromQuery] string userId,
    [FromQuery] long amount) =>
{
    if (amount <= 0) return Results.BadRequest(new { error = "amount must be > 0" });

    var db = mux.GetDatabase();
    var key = BalanceKey(appPrefix, userId);
    var historyKey = WithdrawalHistoryKey(appPrefix, userId);

    var res = (long)await db.ScriptEvaluateAsync(
        RedisScripts.WithdrawScript,
        new RedisKey[] { key, historyKey },
        new RedisValue[] { amount }
    );

    if (res == -1) return Results.NotFound(new { status = "missing" });
    if (res == 0)  return Results.Ok(new { status = "insufficient" });

    return Results.Ok(new { status = "ok", newBalance = res });
});

app.MapPost("/withdraw_medallion_distributed_lock", async (
    [FromServices] IConnectionMultiplexer mux,
    [FromQuery] string userId,
    [FromQuery] long amount) =>
{
    if (amount <= 0) return Results.BadRequest(new { error = "amount must be > 0" });

    var db = mux.GetDatabase();
    var balKey = BalanceKey(appPrefix, userId);
    var lockKey = LockKey(appPrefix, userId);
    var historyKey = WithdrawalHistoryKey(appPrefix, userId);

    var lockTtl = TimeSpan.FromMilliseconds(800);
    var waitBudget = TimeSpan.FromMilliseconds(250);
    var retryDelay = TimeSpan.FromMilliseconds(10);

    var lockHandle = await new Medallion.Threading.Redis.RedisDistributedLock(lockKey, db, options =>
    {
        options.Expiry(lockTtl);
        options.BusyWaitSleepTime(retryDelay, retryDelay);
    }).TryAcquireAsync(waitBudget);

    if (lockHandle == null)
    {
        return Results.Ok(new { status = "lock_busy", error = "Could not acquire lock" });
    }

    try
    {
        await using (lockHandle)
        {
            var currentVal = await db.StringGetAsync(balKey);
            if (!currentVal.HasValue)
                return Results.NotFound(new { status = "missing" });

            if (!long.TryParse(currentVal.ToString(), out var currentBalance))
                return Results.Problem("balance parse error");

            if (currentBalance < amount)
                return Results.Ok(new { status = "insufficient" });

            var newBalance = await db.StringDecrementAsync(balKey, amount);
            await db.ListLeftPushAsync(historyKey, amount);
            return Results.Ok(new { status = "ok", newBalance });
        }
    }
    catch (Exception ex)
    {
        return Results.Problem($"Error during withdrawal: {ex.Message}");
    }
});

app.Run();
