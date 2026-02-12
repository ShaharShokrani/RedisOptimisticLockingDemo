public static class RedisScripts
{
    // Pre-defined Lua scripts as constants for better performance (critical for 10k+ ops/sec)
    // Redis server caches scripts by SHA, but keeping them as constants avoids string allocation
    // For maximum performance, scripts are loaded once and reused
    
    public const string WithdrawScript = @"
        local bal = tonumber(redis.call('GET', KEYS[1]) or '-1')
        local amt = tonumber(ARGV[1])

        if bal < 0 then return -1 end
        if bal < amt then return 0 end

        local newBal = redis.call('DECRBY', KEYS[1], amt)
        return newBal
    ";

    public const string ReleaseLockScript = @"
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
    ";
}

