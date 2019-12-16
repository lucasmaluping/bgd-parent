package com.example.bgdteam2echarts.service.impl;

import com.example.bgdteam2echarts.dao.Android_tableMapper;
import com.example.bgdteam2echarts.model.Android_table;
import com.example.bgdteam2echarts.model.Android_tableExample;
import com.example.bgdteam2echarts.service.IAndroid_tableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author 组1{侯佳伟，张政，王强，云宇庭，于浩，张瑜}
 * @Date 2019/12/11 18:50
 * @Version 1.0
 */
@Service
public class Android_tableServiceImpl implements IAndroid_tableService {

    @Autowired
    Android_tableMapper android_tableMapper;
    @Override
    public List<Android_table> selectByExample() {
        Android_tableExample android_tableExample = new Android_tableExample();
        return android_tableMapper.selectByExample(android_tableExample);
    }
}
