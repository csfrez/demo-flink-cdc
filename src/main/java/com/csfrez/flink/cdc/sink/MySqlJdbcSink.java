package com.csfrez.flink.cdc.sink;

import com.csfrez.flink.cdc.bean.StatementBean;
import com.csfrez.flink.cdc.dao.DaoConnection;
import com.csfrez.flink.cdc.enumeration.OperationTypeEnum;
import com.csfrez.flink.cdc.tool.IOTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.Statement;

@Slf4j
public class MySqlJdbcSink extends RichSinkFunction<StatementBean> {

    // 每来一条数据，调用连接，执行sql
    @Override
    public void invoke(StatementBean value, Context context) throws Exception {
        try {
            Connection connection = DaoConnection.getConnection(value.getDataSourceName());
            if(OperationTypeEnum.INSERT.value().equals(value.getOperationType())){
                System.out.println("INSERT的操作类型++++++++++++++++");
                Statement stmt = connection.createStatement();
                stmt.executeUpdate(value.getSql());
                IOTool.close(stmt);
            } else if(OperationTypeEnum.UPDATE.value().equals(value.getOperationType())){
                System.out.println("UPDATE++++++++++++++++");
                Statement stmt = connection.createStatement();
                stmt.executeUpdate(value.getSql());
                IOTool.close(stmt);
            } else if(OperationTypeEnum.DELETE.value().equals(value.getOperationType())){
                System.out.println("DELETE的操作类型++++++++++++++++");
                Statement stmt = connection.createStatement();
                stmt.executeUpdate(value.getSql());
                IOTool.close(stmt);
            } else {
                System.out.println("不支持的操作类型++++++++++++++++");
            }
        } catch (Exception e){
            e.printStackTrace();
            log.error("MySqlJdbcSink.invoke", e);
        }
    }
}
