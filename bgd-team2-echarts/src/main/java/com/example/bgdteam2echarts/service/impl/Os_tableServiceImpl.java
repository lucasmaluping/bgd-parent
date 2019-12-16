package com.example.bgdteam2echarts.service.impl;

import com.example.bgdteam2echarts.dao.Android_tableMapper;
import com.example.bgdteam2echarts.dao.Os_tableMapper;
import com.example.bgdteam2echarts.model.Android_table;
import com.example.bgdteam2echarts.model.Android_tableExample;
import com.example.bgdteam2echarts.model.Os_table;
import com.example.bgdteam2echarts.model.Os_tableExample;
import com.example.bgdteam2echarts.service.IAndroid_tableService;
import com.example.bgdteam2echarts.service.IOs_tableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author 组1{侯佳伟，张政，王强，云宇庭，于浩，张瑜}
 * @Date 2019/12/11 18:50
 * @Version 1.0
 */
@Service
public class Os_tableServiceImpl implements IOs_tableService {

    @Autowired
    Os_tableMapper os_tableMapper;
    @Override
    public List<Os_table> selectByExample() {
        Os_tableExample os_tableExample = new Os_tableExample();
        return os_tableMapper.selectByExample(os_tableExample);
    }
}
