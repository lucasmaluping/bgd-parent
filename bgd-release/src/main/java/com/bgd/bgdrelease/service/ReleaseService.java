package com.bgd.bgdrelease.service;

import java.util.Map;

public interface ReleaseService {
    //获取总数
    public Integer getDauTotal(String date);
    //获取分时明细
    public Map getDauHourMap(String date);


}
