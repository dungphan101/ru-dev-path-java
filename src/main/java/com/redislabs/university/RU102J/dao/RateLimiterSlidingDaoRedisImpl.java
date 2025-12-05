package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import org.apache.commons.lang3.RandomUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.ZonedDateTime;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        // END CHALLENGE #7
        try (Jedis jedis = jedisPool.getResource()) {
            String key = getKey(name);
            int randomNumber = RandomUtils.nextInt();
            long currentMillis = ZonedDateTime.now().toInstant().toEpochMilli();

            Transaction t = jedis.multi();
            t.zadd(key, currentMillis, currentMillis + ":" + randomNumber);
            t.zremrangeByScore(key, 0, currentMillis - windowSizeMS);
            Response<Long> hits = t.zcard(key);
            t.exec();

            if (hits.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
    }

    private String getKey(String name) {
        return KeyHelper.getKey(String.format("limiter:%d:%s:%d", windowSizeMS, name, maxHits));
    }
}
