package com.csfrez.flink.cdc.bean;

import com.alibaba.fastjson.JSON;
import lombok.Data;

import java.io.Serializable;

@Data
public class StatementBean implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 数据源名称
     */
    private String dataSourceName;

    /**
     * 操作类型：insert,update,delete
     */
    private String operationType;

    /**
     * SQL语句
     */
    private String sql;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
