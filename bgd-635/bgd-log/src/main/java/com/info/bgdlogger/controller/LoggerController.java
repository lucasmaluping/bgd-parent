package com.info.bgdlogger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/***
 * @author :star
 * @time :2019.12.7
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
        JSONObject jsonObject = JSON.parseObject(logJson);
        // 落盘到logfile   log4j
        logger.info(jsonObject.toJSONString());
        //System.out.println(jsonObject.toJSONString());
        // 发送kafka
//        kafkaTemplate.send("bigdata_635", jsonObject.toJSONString());

        return "success";
    }

}
