package com.csfrez.flink.cdc.sink;

import com.csfrez.flink.cdc.bean.StatementBean;
import com.csfrez.flink.cdc.dao.DaoConnection;
import com.csfrez.flink.cdc.enumeration.OperationTypeEnum;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class MySqlJdbcSink extends RichSinkFunction<StatementBean> {

    PreparedStatement insertStmt = null;
    PreparedStatement updateStmt = null;


    // 每来一条数据，调用连接，执行sql
    @Override
    public void invoke(StatementBean value, Context context) throws Exception {
        Connection connection = DaoConnection.getConnection(value.getDataSourceName());
        if(OperationTypeEnum.INSERT.value().equals(value.getOperationType())){
            System.out.println("INSERT的操作类型++++++++++++++++");

        } else if(OperationTypeEnum.UPDATE.value().equals(value.getOperationType())){
            System.out.println("UPDATE++++++++++++++++");
            updateStmt = connection.prepareStatement(value.getSql());
            updateStmt.execute();
        } else if(OperationTypeEnum.DELETE.value().equals(value.getOperationType())){
            System.out.println("DELETE的操作类型++++++++++++++++");

        } else {
            System.out.println("不支持的操作类型++++++++++++++++");
        }
    }
}
