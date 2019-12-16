package com.example.bgdteam2echarts.dao;

import com.example.bgdteam2echarts.model.Android_table;
import com.example.bgdteam2echarts.model.Android_tableExample;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
@Mapper
public interface Android_tableMapper {
    long countByExample(Android_tableExample example);

    int deleteByExample(Android_tableExample example);

    int insert(Android_table record);

    int insertSelective(Android_table record);

    List<Android_table> selectByExampleWithBLOBs(Android_tableExample example);

    List<Android_table> selectByExample(Android_tableExample example);

    int updateByExampleSelective(@Param("record") Android_table record, @Param("example") Android_tableExample example);

    int updateByExampleWithBLOBs(@Param("record") Android_table record, @Param("example") Android_tableExample example);

    int updateByExample(@Param("record") Android_table record, @Param("example") Android_tableExample example);
}