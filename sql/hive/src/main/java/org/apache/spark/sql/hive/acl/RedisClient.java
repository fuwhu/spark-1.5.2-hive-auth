package org.apache.spark.sql.hive.acl;

/**
 * Created by hufuwang on 15/10/26.
 */

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisClient {

    private JedisPool pool;
    private static final Logger LOG = Logger.getLogger(RedisClient.class);

    public RedisClient(String host, int port, int db){
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5000);
        config.setMaxTotal(10000);
        config.setMaxWaitMillis(1000 * 100);
        config.setTestOnBorrow(false);
        pool = new JedisPool(config, host, port, 100000, null, db);
    }

    public void hset(String key, String field, String value) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.hset(key, field, value);
        } catch (Exception e) {
            LOG.error("Failed to hset to redis.");
        } finally {
            returnResource(pool, jedis);
        }
    }

    public static void returnResource(JedisPool pool, Jedis redis) {
        if (redis != null) {
            pool.returnResource(redis);
        }
    }

    public Boolean sismember(String key, String member){
        Jedis jedis = null;
        Boolean isMember = false;
        try{
            jedis = pool.getResource();
            isMember = jedis.sismember(key,member);
        } catch(Exception e){
            LOG.error("Failed to execute sismember to redis.");
            e.printStackTrace();
        }finally{
            returnResource(pool, jedis);
        }
        return isMember;
    }

    public void close(){
        if(pool!=null && !pool.isClosed()){
            pool.close();
        }
    }

}
