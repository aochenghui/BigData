package com.cch.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author HUIHUI
 * @date 2023/9/4 21:21
 */
public class JedisDemo {

    public static void main(String[] args) {
        Jedis jedis = getJedisFromPool();

        String pong = jedis.ping();
        System.out.println(pong);

        //使用完后，一定要关闭
        jedis.close();
    }

    /**
     * 测试每种类型的API
     */
    public static void testString(){
        Jedis jedis = getJedisFromPool();
        jedis.set("k1", "v1");
        String k1 = jedis.get("k1");
        jedis.strlen("k1");
        jedis.incr("k1");
        jedis.decr("k1");

//        jedis.incrby("k1");
//        jedis.decrby("k1");
        jedis.mset("");
        jedis.mget("k1");
        jedis.msetnx("");
        jedis.close();
    }
    public static void testList(){
        Jedis jedis = getJedisFromPool();
        jedis.sadd("");
        jedis.smembers("k4");
        jedis.srem("");
        jedis.srandmember("");
        jedis.spop("");
    }
    public static void testSet(){
        Jedis jedis = getJedisFromPool();
        jedis.sadd("");
        jedis.sinter("");
        jedis.sunion("");
        jedis.sdiff("");
    }
    public static void testZset(){
        Jedis jedis = getJedisFromPool();
        jedis.hexists("","");
        jedis.hget("","");
        jedis.hset("","","");
        jedis.hvals("");
    }
    public static void testHash(){
        Jedis jedis = getJedisFromPool();
        jedis.hexists("","");
        jedis.hget("","");
        jedis.hset("","","");
        jedis.hvals("");
    }

    private static String host = "hadoop102";
    private static int port = 6379;

    /**
     * 创建jedis对象
     */
    public static Jedis getJedis(){
        //基于new的方式
        Jedis jedis = new Jedis(host, port);
        return jedis;
    }

    private static JedisPool jedisPool;
    /**
     * 获取Jedis对象，基于连接池的方式
     */
    public static Jedis getJedisFromPool(){
        if (jedisPool == null){
            //先创建连接池对象
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

            jedisPoolConfig.setMaxTotal(10); //最大可用连接数
            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
            jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong

            jedisPool = new JedisPool(jedisPoolConfig, host, port);
        }
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
}
