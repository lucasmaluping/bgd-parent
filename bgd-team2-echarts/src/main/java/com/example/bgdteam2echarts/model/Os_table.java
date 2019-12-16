package com.example.bgdteam2echarts.model;

import org.springframework.stereotype.Component;


public class Os_table {
    private Integer num;

    private String os;

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os == null ? null : os.trim();
    }

    @Override
    public String toString() {
        return "Os_table{" +
                "num=" + num +
                ", os='" + os + '\'' +
                '}';
    }
}