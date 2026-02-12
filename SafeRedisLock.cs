using System;
using System.Threading.Tasks;
using StackExchange.Redis;

/// <summary>
/// Safe Redis distributed lock implementation using token verification.
/// Used by Strategy 3: withdraw_polling_token endpoint.
/// 
/// Features:
/// - Token-based lock (prevents accidental release)
/// - Configurable TTL, timeout, and retry delay
/// - Throws exception on timeout (caller must catch)
/// - Safe release using Lua script (token verification)
/// </summary>
public class SafeRedisLock : IAsyncDisposable
{
    private string? _lockKey;
    private string? _token;
    private readonly IDatabase _database;

    public SafeRedisLock(IDatabase database)
    {
        _database = database;
    }

    /// <summary>
    /// Acquires a distributed lock with polling retry.
    /// </summary>
    /// <param name="lockKey">Redis key to use as the lock</param>
    /// <param name="ttl">Time-to-live for the lock (must exceed critical section duration)</param>
    /// <param name="timeout">Maximum time to wait for lock acquisition</param>
    /// <param name="retryDelay">Delay between retry attempts (default: 10ms)</param>
    /// <returns>IAsyncDisposable that releases the lock when disposed</returns>
    /// <exception cref="Exception">Thrown if lock cannot be acquired within timeout</exception>
    public async Task<IAsyncDisposable> LockAsync(string lockKey, TimeSpan ttl, TimeSpan timeout, TimeSpan retryDelay = default)
    {
        _lockKey = lockKey;
        _token = Guid.NewGuid().ToString("N"); // Unique token prevents accidental lock release
        var startTime = DateTime.UtcNow;
        
        // Default retry delay to 10ms if not specified
        if (retryDelay == default)
            retryDelay = TimeSpan.FromMilliseconds(10);

        // Polling loop: try to acquire lock until timeout
        // Uses SET with NX (Not Exists) - atomic check-and-set operation
        while (!await _database.StringSetAsync(_lockKey, _token, ttl, When.NotExists))
        {
            await Task.Delay(retryDelay);
            if ((DateTime.UtcNow - startTime).TotalSeconds > timeout.TotalSeconds)
                throw new Exception($"Redis lock timeout after: {timeout.TotalSeconds} second(s)");
        }

        return this;
    }

    /// <summary>
    /// Releases the lock safely using Lua script.
    /// Only deletes lock if token matches (prevents deleting someone else's lock).
    /// </summary>
    private Task ReleaseAsync()
    {
        if (_lockKey == null || _token == null)
            return Task.CompletedTask;
        
        // Lua script ensures atomic check-and-delete with token verification
        // This is critical: prevents releasing a lock that was re-acquired by another process
        return _database.ScriptEvaluateAsync(
            RedisScripts.ReleaseLockScript,
            new RedisKey[] { _lockKey },
            new RedisValue[] { _token }
        );
    }

    public ValueTask DisposeAsync() => new(ReleaseAsync());
}

