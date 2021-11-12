package com.csfrez.flink.cdc.demo;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.csfrez.flink.cdc.bean.BinlogBean;
import com.csfrez.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.csfrez.flink.cdc.function.BinlogProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author yangzhi
 * @date 2021/11/10
 * @email yangzhi@ddjf.com.cn
 */
public class MySqlSourceExample {

    public static void main(String[] args) throws Exception {
        try{
            SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                    .hostname("115.159.34.194")
                    .port(3306)
                    .databaseList("mydb") // monitor all tables under inventory database
                    .username("root")
                    .password("123456")
                    .startupOptions(StartupOptions.latest())
                    .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
//                    .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                    .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
            SingleOutputStreamOperator<BinlogBean> singleOutputStreamOperator = dataStreamSource.process(new BinlogProcessFunction());

            singleOutputStreamOperator.print();

            //env.addSource(sourceFunction).print().setParallelism(1);
            // use parallelism 1 for sink to keep message ordering

            env.execute();
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
