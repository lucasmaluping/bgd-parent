package com.example.bgdteam2echarts.controller;

import com.example.bgdteam2echarts.model.Android_table;
import com.example.bgdteam2echarts.model.Ios_table;
import com.example.bgdteam2echarts.model.Os_table;
import com.example.bgdteam2echarts.service.IAndroid_tableService;
import com.example.bgdteam2echarts.service.IOs_tableService;
//import com.example.bgdteam2echarts.service.IRedisService;
import com.example.bgdteam2echarts.service.Iios_tableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * @Author 组1{侯佳伟，张政，王强，云宇庭，于浩，张瑜}
 * @Date 2019/12/11 16:29
 * @Version 1.0
 */
@Controller
public class IndexController {
    @Autowired
    IAndroid_tableService android_tableService;
    @Autowired
    Iios_tableService iios_tableService;
    @Autowired
    IOs_tableService iOs_tableService;

    @RequestMapping("")
    public String getIndex(){
        return "index";
    }

    @RequestMapping(value = "data.json")
    @ResponseBody
    public List<Android_table> getAndroidData(){
        List<Android_table> android_tables = android_tableService.selectByExample();
        System.out.println(android_tables);
        return android_tables;
    }

    @RequestMapping(value = "ios.json")
    @ResponseBody
    public List<Ios_table> getIosData(){
        List<Ios_table> ios_tables = iios_tableService.selectByExample();
        return ios_tables;
    }

    @RequestMapping("os.json")
    @ResponseBody
    public List<Os_table> getOsData(){
        List<Os_table> os_tables = iOs_tableService.selectByExample();
        System.out.println(os_tables);
        return os_tables;
    }

}
