package com.example.bgdteam2echarts.service;

import com.example.bgdteam2echarts.model.Ios_table;
import com.example.bgdteam2echarts.model.Os_table;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Thomas:侯佳伟，张瑜，张政，云宇庭，王强，杨福长，于浩
 * @version 1.0
 * @date 2019/12/16 10:59
 */

public interface Iios_tableService {
    List<Ios_table> selectByExample();
}
