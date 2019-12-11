package com.demo.controller;

/**
 * @Author Lucas
 * @Date 2019/12/9 15:41
 * @Version 1.0
 */
import com.bgd.common.constant.MyRedisUtil;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.Mapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

@RestController
public class ShowController {
    /**
     * 获取数据
     * 把数据发送到前台显示
     * @return
     */
    @RequestMapping("/getData.json")
    public String getData() {
        Jedis jedisClient = MyRedisUtil.getJedisClient();
//        jedisClient
        return null;
    }
}
