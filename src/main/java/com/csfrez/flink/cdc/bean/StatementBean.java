package com.csfrez.flink.cdc.bean;

import lombok.Data;

@Data
public class StatementBean {

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

}
