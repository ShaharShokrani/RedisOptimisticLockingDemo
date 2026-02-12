using System;
using System.Threading.Tasks;
using StackExchange.Redis;

/// <summary>
/// Redis distributed lock implementation using polling retry pattern (UNSAFE VERSION).
/// Matches production code style - uses direct DEL without token verification.
/// Used by Strategy 4: withdraw_polling endpoint.
/// 
/// WARNING: This implementation has a race condition!
/// - Uses empty string as lock value (no token)
/// - Direct DEL without token verification
/// - Wrong process can delete the lock if TTL expires
/// 
/// Features:
/// - Simple lock acquisition (no token)
/// - Configurable TTL, timeout, and retry delay
/// - Default 10ms retry delay (matches Strategy 3 for fair comparison)
/// - UNSAFE release: Direct DEL without verification
/// </summary>
public class UnsafeRedisLock : IAsyncDisposable
{
    private string? _lockKey;
    private readonly IDatabase _database;

    public UnsafeRedisLock(IDatabase database)
    {
        _database = database;
    }

    /// <summary>
    /// Acquires a distributed lock with polling retry.
    /// </summary>
    /// <param name="lockKey">Redis key to use as the lock</param>
    /// <param name="ttl">Time-to-live for the lock (must exceed critical section duration)</param>
    /// <param name="timeout">Maximum time to wait for lock acquisition</param>
    /// <param name="retryDelay">Delay between retry attempts (default: 10ms, matches Strategy 3 for comparison)</param>
    /// <returns>IAsyncDisposable that releases the lock when disposed</returns>
    /// <exception cref="Exception">Thrown if lock cannot be acquired within timeout</exception>
    public async Task<IAsyncDisposable> LockAsync(string lockKey, TimeSpan ttl, TimeSpan timeout, TimeSpan retryDelay = default)
    {
        _lockKey = lockKey;
        var startTime = DateTime.UtcNow;
        
        // Default retry delay to 10ms if not specified (matches Strategy 3 for fair comparison)
        if (retryDelay == default)
            retryDelay = TimeSpan.FromMilliseconds(10);

        // Polling loop: try to acquire lock until timeout
        // Uses SET with NX (Not Exists) - atomic check-and-set operation
        // Uses empty string as value (no token, matches production code)
        while (!await _database.StringSetAsync(_lockKey, string.Empty, ttl, When.NotExists))
        {
            await Task.Delay(retryDelay);
            if ((DateTime.UtcNow - startTime).TotalSeconds > timeout.TotalSeconds)
                throw new Exception($"Redis lock timeout after: {timeout.TotalSeconds} second(s)");
        }

        return this;
    }

    /// <summary>
    /// Releases the lock UNSAFELY using direct DEL without token verification.
    /// 
    /// RACE CONDITION: If the lock TTL expires and another process acquires the lock,
    /// this will delete their lock, not ours!
    /// </summary>
    private Task ReleaseAsync()
    {
        if (_lockKey == null)
            return Task.CompletedTask;
        
        // UNSAFE: Direct deletion without token verification (matches production code)
        // This demonstrates the race condition where wrong process can delete the lock
        return _database.KeyDeleteAsync(_lockKey);
    }

    public ValueTask DisposeAsync() => new(ReleaseAsync());
}

