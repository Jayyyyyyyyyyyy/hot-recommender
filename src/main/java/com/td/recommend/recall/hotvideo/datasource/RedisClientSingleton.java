package com.td.recommend.recall.hotvideo.datasource;

import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import com.typesafe.config.Config;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;


public class RedisClientSingleton {
    private static final Logger log = LoggerFactory.getLogger(RedisClientSingleton.class);
    public static final RedisClientSingleton cf = new RedisClientSingleton("cf-server");
    public static final RedisClientSingleton origin = new RedisClientSingleton("origin-server");
    public static final RedisClientSingleton highctr = new RedisClientSingleton("highctr-server");
    public static final RedisClientSingleton search = new RedisClientSingleton("search-server");
    public static final RedisClientSingleton popular = new RedisClientSingleton("popular-server");
    public static final RedisClientSingleton headpoolFilter = new RedisClientSingleton("headpool-filter-server");
    public static final RedisClientSingleton hot_mp3 = new RedisClientSingleton("hot_mp3-server");
    public static final RedisClientSingleton general = new RedisClientSingleton("general-server");
    public static final RedisClientSingleton tr_day_inview = new RedisClientSingleton("tr_day_inview-server");
//    public static final RedisClientSingleton looklike = new RedisClientSingleton("looklike-server");

    private JedisPool jedisPool;

    private RedisClientSingleton(String serverConf) {
        Config redisConf = HotVideoConfig.getInstance().getConfig().getConfig(serverConf);
        String redisHost = redisConf.getString("jedis.host");
        int redisPort = redisConf.getInt("jedis.port");
        int timeout = redisConf.getInt("jedis.timeout");
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(redisConf.getInt("jedis.pool.config.maxTotal"));
        poolConfig.setMaxIdle(redisConf.getInt("jedis.pool.config.maxIdle"));
        poolConfig.setMinIdle(redisConf.getInt("jedis.pool.config.minIdle"));
        poolConfig.setTestOnBorrow(redisConf.getBoolean("jedis.pool.config.testOnBorrow"));
        poolConfig.setTestOnReturn(redisConf.getBoolean("jedis.pool.config.testOnReturn"));
        poolConfig.setTestWhileIdle(redisConf.getBoolean("jedis.pool.config.testWhileIdle"));
        poolConfig.setMaxWaitMillis(redisConf.getLong("jedis.pool.config.maxWaitMillis"));
        poolConfig.setMinEvictableIdleTimeMillis(redisConf.getLong("jedis.pool.config.minEvictableIdleTimeMillis"));
        poolConfig.setTimeBetweenEvictionRunsMillis(redisConf.getLong("jedis.pool.config.timeBetweenEvictionRunsMillis"));
        poolConfig.setNumTestsPerEvictionRun(redisConf.getInt("jedis.pool.config.numTestsPerEvictionRun"));
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, timeout);
        log.info("init jedis pool: {}:{} {}", new Object[]{redisHost, Integer.valueOf(redisPort), poolConfig});
    }

    public String get(String key) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.get(key);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }
    public byte[] get(byte[] key) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.get(key);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }
    public Long ttl(String key) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.ttl(key);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }
    public List<String> lrange(String key, long start, long end) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.lrange(key, start, end);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }
    public List<String> mget(String... keys) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.mget(keys);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public Set<Tuple> zrange(String key, long start, long end) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.zrangeWithScores(key, start, end);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public Set<String> smembers(String key) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.smembers(key);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public Boolean exists(String key) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.exists(key);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public Map<String,String> hgetAll(String key) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hgetAll(key);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }


    public void expire(String key, int seconds) {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.expire(key, seconds);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public void rpush(String key, List<String> values) {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.rpush(key, values.toArray(new String[0]));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void main(String[] args) {
        List<String> top = RedisClientSingleton.cf.lrange("r_icf_topn_8369578", 0, 10);
        System.out.println(top);
    }

}
