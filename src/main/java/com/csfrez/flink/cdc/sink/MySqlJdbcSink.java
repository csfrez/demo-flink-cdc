package com.csfrez.flink.cdc.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.csfrez.flink.cdc.bean.PrepareStatementBean;
import com.csfrez.flink.cdc.bean.StatementBean;
import com.csfrez.flink.cdc.config.CommonConfig;
import com.csfrez.flink.cdc.enumeration.OperationTypeEnum;
import com.csfrez.flink.cdc.tool.DruidDataSourceTool;
import com.csfrez.flink.cdc.tool.IOTool;
import com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;

@Slf4j
public class MySqlJdbcSink extends RichSinkFunction<StatementBean> {

    private static final long serialVersionUID = -2080964448675323976L;

    private static final FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    private static Map<String, DruidDataSource> dataSourceMap = new ConcurrentHashMap<>();

    private String active = "";

    public MySqlJdbcSink(String active){
        this.active = active;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        List<String> nameList = CommonConfig.getSinkOutDatasourceList();
        for (String name : nameList) {
            log.info("MySqlJdbcSink初始化数据源==>>" + name + "==>>" + active);
            DruidDataSource druidDataSource = DruidDataSourceTool.getDruidDataSource(name, active);
            if (druidDataSource != null) {
                dataSourceMap.put(name, druidDataSource);
            }
        }
    }

    // 每来一条数据，调用连接，执行sql
    @Override
    public void invoke(StatementBean value, Context context) throws Exception {
        long start = System.currentTimeMillis();
        int count = 0;
        try {
            count = this.executeUpdate(value);
        } catch (Exception e){
            log.error("MySqlJdbcSink.invoke", e);
        } finally {
            long duration = System.currentTimeMillis() - start;
            log.info("sink结束时间：" + LocalDateTime.now() + "==>>花费时间：" + duration + "毫秒，count=" + count);
        }
    }

    private int executeUpdate(StatementBean value){
        PreparedStatement pstmt = null;
        Statement stmt = null;
        Connection connection = null;
        try {
            connection = dataSourceMap.get(value.getDataSourceName()).getConnection();
            String deleteSql = value.getDeleteSql();
            if(OperationTypeEnum.INSERT.value().equals(value.getOperationType()) && StringUtils.isNotEmpty(deleteSql)){
                stmt = connection.createStatement();
                stmt.executeUpdate(deleteSql);
            }
            if (value instanceof PrepareStatementBean) {
                PrepareStatementBean prepareStatementBean = (PrepareStatementBean) value;
                String sql = this.replacePlaceholder(value.getSql(), prepareStatementBean.getParamList());
                log.info("sql={}", sql);
                pstmt = connection.prepareStatement(value.getSql());
                this.setParam(prepareStatementBean.getParamList(), pstmt);
                return pstmt.executeUpdate();
            } else {
                stmt = connection.createStatement();
                return stmt.executeUpdate(value.getSql());
            }
        } catch (MySQLTransactionRollbackException e){
            e.printStackTrace();
            log.error("MySqlJdbcSink.MySQLTransactionRollbackException", e);
            // 回滚返回错误码-1
            return -1;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("MySqlJdbcSink.executeUpdate", e);
        } finally {
            IOTool.close(pstmt);
            IOTool.close(stmt);
            IOTool.close(connection);
        }
        return 0;
    }

    /**
     * 设置参数
     * @param paramList
     * @param pstmt
     * @throws SQLException
     */
    private void setParam(List<PrepareStatementBean.Param<?>> paramList, PreparedStatement pstmt) throws SQLException {
        if (paramList.isEmpty()) {
            return;
        }
        paramList.sort(Comparator.comparingInt(PrepareStatementBean.Param::getIndex));
        for (PrepareStatementBean.Param<?> param : paramList) {
            if (param.getValue() == null) {
                pstmt.setString(param.getIndex(), null);
                continue;
            }
            if (param.getType() == String.class) {
                PrepareStatementBean.Param<String> newParam = (PrepareStatementBean.Param<String>) param;
                pstmt.setString(newParam.getIndex(), newParam.getValue());
            } else if (param.getType() == Long.class) {
                PrepareStatementBean.Param<Long> newParam = (PrepareStatementBean.Param<Long>) param;
                pstmt.setLong(newParam.getIndex(), newParam.getValue());
            } else if (param.getType() == Integer.class) {
                PrepareStatementBean.Param<Integer> newParam = (PrepareStatementBean.Param<Integer>) param;
                pstmt.setInt(newParam.getIndex(), newParam.getValue());
            } else if (param.getType() == BigDecimal.class) {
                PrepareStatementBean.Param<BigDecimal> newParam = (PrepareStatementBean.Param<BigDecimal>) param;
                pstmt.setBigDecimal(newParam.getIndex(), newParam.getValue());
            } else if (param.getType() == Date.class) {
                PrepareStatementBean.Param<Date> newParam = (PrepareStatementBean.Param<Date>) param;
                pstmt.setTimestamp(newParam.getIndex(), new Timestamp(newParam.getValue().getTime()));
            }
        }
    }

    /**
     * SQL语句打印
     * @param boundSql
     * @param paramList
     * @return
     */
    private String replacePlaceholder(String boundSql, List<PrepareStatementBean.Param<?>> paramList) {
        try {
            String sql = boundSql.replaceAll("[\\s]+", " ");
            String result;
            Object propertyValue;
            for (PrepareStatementBean.Param<?> param : paramList) {
                propertyValue = param.getValue();
                if (propertyValue != null) {
                    if (propertyValue instanceof String) {
                        result = "'" + propertyValue + "'";
                    } else if (propertyValue instanceof Date) {
                        result = "'" + DATE_FORMAT.format(propertyValue) + "'";
                    } else {
                        result = propertyValue.toString();
                    }
                } else {
                    result = "null";
                }
                sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(result));
            }
            return sql;
        } catch (Exception e){
            e.printStackTrace();
            log.info("MySqlJdbcSink.replacePlaceholder", e);
        }
        return null;
    }
}
