package com.csfrez.flink.cdc.dao;

import com.csfrez.flink.cdc.config.TableConfig;
import com.csfrez.flink.cdc.thread.StringThreadLocal;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;

/**
 * @author yangzhi
 * @date 2021/11/16
 * @email csfrez@163.com
 */
@Slf4j
public abstract class BaseDao {

    public static Connection getConnection(String tableName){
        String active = StringThreadLocal.get();
        String dataSourceName = TableConfig.getTableConfig(tableName).getDataSourceName();
        return DaoConnection.getConnection(dataSourceName, active);
    }

    public static String[] getColumns(String tableName){
        return TableConfig.getTableConfig(tableName).getColumns().split(",");
    }
}
