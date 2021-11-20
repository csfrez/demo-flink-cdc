package com.csfrez.flink.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.csfrez.flink.cdc.bean.StatementBean;
import com.csfrez.flink.cdc.config.SourceConfig;
import com.csfrez.flink.cdc.config.TableConfig;
import com.csfrez.flink.cdc.debezium.FastjonDeserializationSchema;
import com.csfrez.flink.cdc.function.BinlogFilterFunction;
import com.csfrez.flink.cdc.function.BinlogFlatMapFunction;
import com.csfrez.flink.cdc.sink.MySqlJdbcSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Map;

/**
 * @author yangzhi
 * @date 2021/11/10
 * @email csfrez@163.com
 */
@Slf4j
public class MySqlSourceApplication {


    public static void main(String[] args) throws Exception {
        try{
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String active = parameterTool.get("ddjf.profiles.active", "");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            processSourceFunction(env, active);
            env.execute();
        } catch (Exception e){
            log.error("MySqlSourceApplication", e);
            e.printStackTrace();
        }

    }

    /**
     * 多数据源列表
     * @return
     */
    private static void processSourceFunction(StreamExecutionEnvironment env, String active){
        SourceConfig.init(active);
        Map<String, SourceConfig> sourceConfigMap = SourceConfig.getSourceConfig();
        List<String> tableList = TableConfig.getTableList();
        for(Map.Entry<String, SourceConfig> entry: sourceConfigMap.entrySet()) {
            SourceConfig sourceConfig = entry.getValue();
            System.out.println(entry.getKey() + "===>" + sourceConfig);
            SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                    .hostname(sourceConfig.getHostname())
//                    .serverTimeZone("Europe/London")
                    .port(sourceConfig.getPort())
                    .databaseList(sourceConfig.getDatabase()) // monitor all tables under inventory database
                    .tableList(tableList.toArray(new String[tableList.size()]))
                    .username(sourceConfig.getUsername())
                    .password(sourceConfig.getPassword())
                    .startupOptions(StartupOptions.latest())
//                    .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
                    .deserializer(new FastjonDeserializationSchema()) // converts SourceRecord to String
                    .build();
            DataStream<String> dataStream = env.addSource(sourceFunction, entry.getKey());
            DataStream<String> filterDataStream = dataStream.filter(new BinlogFilterFunction());
            DataStream<StatementBean> flatMapDataStream = filterDataStream.flatMap(new BinlogFlatMapFunction(active));
            flatMapDataStream.print();
            flatMapDataStream.addSink(new MySqlJdbcSink(active));
        }
    }
}
