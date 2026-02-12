public static class RedisScripts
{
    public const string WithdrawScript = @"
        local bal = tonumber(redis.call('GET', KEYS[1]) or '-1')
        local amt = tonumber(ARGV[1])

        if bal < 0 then return -1 end
        if bal < amt then return 0 end

        local newBal = redis.call('DECRBY', KEYS[1], amt)
        redis.call('LPUSH', KEYS[2], ARGV[1])
        redis.call('LTRIM', KEYS[2], 0, 999)
        return newBal
    ";
}
