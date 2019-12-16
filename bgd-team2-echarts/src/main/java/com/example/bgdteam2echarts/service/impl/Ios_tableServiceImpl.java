package com.example.bgdteam2echarts.service.impl;

import com.example.bgdteam2echarts.dao.Ios_tableMapper;
import com.example.bgdteam2echarts.model.Ios_table;
import com.example.bgdteam2echarts.model.Ios_tableExample;
import com.example.bgdteam2echarts.model.Os_table;
import com.example.bgdteam2echarts.service.Iios_tableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Thomas:侯佳伟，张瑜，张政，云宇庭，王强，杨福长，于浩
 * @version 1.0
 * @date 2019/12/16 11:00
 */
@Service
public class Ios_tableServiceImpl implements Iios_tableService {

    @Autowired
    Ios_tableMapper ios_tableMapper;

    @Override
    public List<Ios_table> selectByExample() {

        return ios_tableMapper.selectByExample(new Ios_tableExample());
    }
}
