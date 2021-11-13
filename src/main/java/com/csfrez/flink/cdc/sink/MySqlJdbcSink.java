package com.csfrez.flink.cdc.sink;

import com.alibaba.druid.util.jdbc.PreparedStatementBase;
import com.csfrez.flink.cdc.bean.PrepareStatementBean;
import com.csfrez.flink.cdc.bean.StatementBean;
import com.csfrez.flink.cdc.dao.DaoConnection;
import com.csfrez.flink.cdc.enumeration.OperationTypeEnum;
import com.csfrez.flink.cdc.tool.IOTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

@Slf4j
public class MySqlJdbcSink extends RichSinkFunction<StatementBean> {

    // 每来一条数据，调用连接，执行sql
    @Override
    public void invoke(StatementBean value, Context context) throws Exception {
        PreparedStatement pstmt = null;
        Statement stmt = null;
        try {
            Connection connection = DaoConnection.getConnection(value.getDataSourceName());
            System.out.println("操作类型为===>>" + value.getOperationType());
            System.out.println("SQL===>>" + value.getSql());

            if(value instanceof PrepareStatementBean){
                PrepareStatementBean prepareStatementBean = (PrepareStatementBean)value;
                pstmt = connection.prepareStatement(value.getSql());
                this.setParam(prepareStatementBean.getParamList(), pstmt);
                pstmt.executeUpdate();
                IOTool.close(pstmt);
            } else {
                stmt = connection.createStatement();
                stmt.executeUpdate(value.getSql());
                IOTool.close(stmt);
            }
        } catch (Exception e){
            e.printStackTrace();
            log.error("MySqlJdbcSink.invoke", e);
        } finally {
            IOTool.close(pstmt);
            IOTool.close(stmt);
        }
    }

    private void setParam(List<PrepareStatementBean.Param<?>> paramList, PreparedStatement pstmt) throws SQLException {
        if(paramList.isEmpty()){
            return;
        }
        paramList.sort(Comparator.comparingInt(PrepareStatementBean.Param::getIndex));
        for(PrepareStatementBean.Param<?> param: paramList){
            if(param.getValue() == null){
                pstmt.setString(param.getIndex(), null);
                continue;
            }
            if(param.getType() == String.class){
                PrepareStatementBean.Param<String> newParam = (PrepareStatementBean.Param<String>)param;
                pstmt.setString(newParam.getIndex(), newParam.getValue());
            } else if(param.getType() == Long.class){
                PrepareStatementBean.Param<Long> newParam = (PrepareStatementBean.Param<Long>)param;
                pstmt.setLong(newParam.getIndex(), newParam.getValue());
            } else if(param.getType() == Integer.class){
                PrepareStatementBean.Param<Integer> newParam = (PrepareStatementBean.Param<Integer>)param;
                pstmt.setInt(newParam.getIndex(), newParam.getValue());
            } else if(param.getType() == BigDecimal.class){
                PrepareStatementBean.Param<BigDecimal> newParam = (PrepareStatementBean.Param<BigDecimal>)param;
                pstmt.setBigDecimal(newParam.getIndex(), newParam.getValue());
            } else if(param.getType() == Date.class){
                PrepareStatementBean.Param<Date> newParam = (PrepareStatementBean.Param<Date>)param;
                pstmt.setTimestamp(newParam.getIndex(), new Timestamp(newParam.getValue().getTime()));
            }
        }
    }
}
