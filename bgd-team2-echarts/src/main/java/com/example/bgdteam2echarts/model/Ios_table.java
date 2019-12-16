package com.example.bgdteam2echarts.model;

public class Ios_table {
    private Integer num;

    private String vs;

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public String getVs() {
        return vs;
    }

    public void setVs(String vs) {
        this.vs = vs == null ? null : vs.trim();
    }
}