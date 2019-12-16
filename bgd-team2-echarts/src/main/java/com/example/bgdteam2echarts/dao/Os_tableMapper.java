package com.example.bgdteam2echarts.dao;

import com.example.bgdteam2echarts.model.Os_table;
import com.example.bgdteam2echarts.model.Os_tableExample;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
@Mapper
public interface Os_tableMapper {
    long countByExample(Os_tableExample example);

    int deleteByExample(Os_tableExample example);

    int insert(Os_table record);

    int insertSelective(Os_table record);

    List<Os_table> selectByExampleWithBLOBs(Os_tableExample example);

    List<Os_table> selectByExample(Os_tableExample example);

    int updateByExampleSelective(@Param("record") Os_table record, @Param("example") Os_tableExample example);

    int updateByExampleWithBLOBs(@Param("record") Os_table record, @Param("example") Os_tableExample example);

    int updateByExample(@Param("record") Os_table record, @Param("example") Os_tableExample example);
}