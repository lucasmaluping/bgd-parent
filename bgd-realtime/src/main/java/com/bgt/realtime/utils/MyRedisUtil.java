package com.bgt.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MyRedisUtil {
    private static JedisPool jedisPool;
    private static Properties properties;
    private static String proPath = "config.properties";
    private static boolean authFlag;
    private static String password;

    static {
        //读取配置文件
        try {
            readConfigFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 配置连接池
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(20);
        poolConfig.setMinIdle(10);
        poolConfig.setMaxTotal(30);
        poolConfig.setMaxWaitMillis(3000);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        jedisPool = new JedisPool(properties.getProperty("redis.host"), Integer.valueOf(properties.getProperty("redis.port")));

        //判断是否需要密码验证
        if ("1".equals(properties.getProperty("redis.authFlag"))) {
            authFlag = true;
            password = properties.getProperty("redis.password");
            if (password == null) {
                try {
                    throw new Exception("redis密码没有读取到！");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static Properties readConfigFile() throws IOException {
        InputStream in = MyRedisUtil.class.getClass().getResourceAsStream("/" + proPath);
        if (in == null) {
            throw new IOException("读取不到配置文件：" + proPath);
        }
        properties = new Properties();
        properties.load(in);
        in.close();
        return properties;
    }

    public static Jedis getJedisClient() {
        Jedis jedis = jedisPool.getResource();
        if (authFlag) {
            jedis.auth(password);
        }
        return jedis;
    }

    public static void close() {
        jedisPool.close();
    }
}
