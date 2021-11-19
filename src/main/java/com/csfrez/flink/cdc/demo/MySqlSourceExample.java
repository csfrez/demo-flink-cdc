package com.csfrez.flink.cdc.demo;

import com.csfrez.flink.cdc.bean.StatementBean;
import com.csfrez.flink.cdc.debezium.FastjonDeserializationSchema;
import com.csfrez.flink.cdc.function.BinlogFilterFunction;
import com.csfrez.flink.cdc.function.BinlogFlatMapFunction;
import com.csfrez.flink.cdc.sink.MySqlJdbcSink;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yangzhi
 * @date 2021/11/10
 * @email yangzhi@ddjf.com.cn
 */
public class MySqlSourceExample {

    public static void main(String[] args) throws Exception {
        try{
            MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                    .hostname("115.159.34.194")
                    .port(3306)
                    .databaseList("mydb") // monitor all tables under inventory database
                    .tableList("mydb.orders") // set captured table
                    .username("root")
                    .password("123456")
                    .startupOptions(StartupOptions.latest())
//                    .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
                    .deserializer(new FastjonDeserializationSchema()) // converts SourceRecord to String
//                    .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                    .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // enable checkpoint
            env.enableCheckpointing(3000);

            DataStream<String> dataStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source").setParallelism(1);
//            dataStream.print().setParallelism(1);

            DataStream<String> filterDataStream = dataStream.filter(new BinlogFilterFunction());
            DataStream<StatementBean> flatMapDataStream = filterDataStream.flatMap(new BinlogFlatMapFunction());

            flatMapDataStream.print();
            flatMapDataStream.addSink(new MySqlJdbcSink());

            env.execute();
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
