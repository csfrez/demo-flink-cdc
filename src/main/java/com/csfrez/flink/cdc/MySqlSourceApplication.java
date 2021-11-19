package com.csfrez.flink.cdc;

import com.csfrez.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySqlSourceApplication {

    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("115.159.34.194")
                .port(3306)
                .databaseList("mydb") // set captured database
                .tableList("mydb.orders") // set captured table
                .username("root")
                .password("123456")
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.latest())
//                .deserializer(new FastjonDeserializationSchema()) // converts SourceRecord to JSON String
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        DataStream<String> dataStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // use parallelism 1 for sink to keep message ordering
        dataStream.print().setParallelism(1);

//        dataStream.addSink(new StringSink());

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
