package com.cch.redis;

import redis.clients.jedis.Jedis;

public class JedisTest {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("hadoop102",6379);
        String ping = jedis.ping();
        System.out.println(ping);
    }
}