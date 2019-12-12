package com.bgd.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bgd.common.constant.MoveConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

/***
 * @author :star
 * @time :2019.12.7
 * 已请求
 * @message: 该类的作用是为了接收mock产生的数据，并将其转换成json字符串的格式
 *            下沉到kafka中
 */
@RestController // Controller+Responsebody
public class LoggerController {

    //使用的是springboot集成的kafka插件
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    //获取一个logger，用于数据的保存
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class);

    //@RequestMapping(value = "/log",method = RequestMethod.POST) =>
    @PostMapping("/log")
    public String dolog(@RequestParam("log") String logJson) {
        // 补时间戳
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts", System.currentTimeMillis());
        // 落盘到logfile   log4j
        logger.info(jsonObject.toJSONString());
        System.out.println(jsonObject.toJSONString());
        String[] arr = {"aa","bb","cc","dd","ee","ff","gg"};
        Random random = new Random();
        int i = random.nextInt(arr.length);
        // 发送kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            //将数据下沉到kafka。KAFKA_TOPIC_STARTUP=bigdata
//            kafkaTemplate.send(MoveConstant.KAFKA_TOPIC_STARTUP, jsonObject.toJSONString());
//            System.out.println(jsonObject);

            kafkaTemplate.send(MoveConstant.KAFKA_TOPIC_Window, arr[i]);
        }
        return "success";
    }

}
