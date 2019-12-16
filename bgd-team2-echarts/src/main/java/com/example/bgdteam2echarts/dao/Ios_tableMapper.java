package com.example.bgdteam2echarts.dao;

import com.example.bgdteam2echarts.model.Ios_table;
import com.example.bgdteam2echarts.model.Ios_tableExample;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
@Mapper
public interface Ios_tableMapper {
    long countByExample(Ios_tableExample example);

    int deleteByExample(Ios_tableExample example);

    int insert(Ios_table record);

    int insertSelective(Ios_table record);

    List<Ios_table> selectByExampleWithBLOBs(Ios_tableExample example);

    List<Ios_table> selectByExample(Ios_tableExample example);

    int updateByExampleSelective(@Param("record") Ios_table record, @Param("example") Ios_tableExample example);

    int updateByExampleWithBLOBs(@Param("record") Ios_table record, @Param("example") Ios_tableExample example);

    int updateByExample(@Param("record") Ios_table record, @Param("example") Ios_tableExample example);
}