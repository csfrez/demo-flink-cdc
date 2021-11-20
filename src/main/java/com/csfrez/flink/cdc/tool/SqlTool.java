package com.csfrez.flink.cdc.tool;

import com.csfrez.flink.cdc.config.TableConfig;

/**
 * @author lihuanliang
 * @create 2021-11-19 9:22
 */
public class SqlTool {
    private static String sql = null;
    public static String concatSql(String tableName,String filterField){
        sql = sql = "SELECT " + TableConfig.getTableConfig(tableName).getColumns() + " FROM "+ tableName + " WHERE " + filterField +" = ?";
        return sql;
    }

    public static void main(String[] args) {
        System.out.println(concatSql("bpms.sys_team", "code"));
    }
}
