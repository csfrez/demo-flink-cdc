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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.checkpoint.Checkpoint;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Savepoint;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
            Properties properties = new Properties();
            properties.setProperty("snapshot.mode", "schema_only");
            SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                    .hostname(sourceConfig.getHostname())
//                    .serverTimeZone("Europe/London")
                    .port(sourceConfig.getPort())
                    .databaseList(sourceConfig.getDatabase()) // monitor all tables under inventory database
                    .tableList(tableList.toArray(new String[tableList.size()]))
                    .username(sourceConfig.getUsername())
                    .password(sourceConfig.getPassword())
//                    .startupOptions(StartupOptions.timestamp(1639831160000L))
                    .startupOptions(StartupOptions.specificOffset("mysql-bin.000008", 156431))
//                    .startupOptions(StartupOptions.latest())
//                    .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
                    .deserializer(new FastjonDeserializationSchema()) // converts SourceRecord to String
//                    .debeziumProperties(properties)
                    .build();

            //2.Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在 CK,如果想要做到断点续传,需要从 Checkpoint 或者 Savepoint 启动程序
            //2.1 开启 Checkpoint,每隔 5 秒钟做一次 CK
            env.enableCheckpointing(5000L);
            //2.2 指定 CK 的一致性语义
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            //2.3 设置任务关闭的时候保留最后一次 CK 数据
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);

            //2.4 指定从 CK 自动重启策略
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
            //2.5 设置状态后端
            env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc"));
            //2.6 设置访问 HDFS 的用户名
            System.setProperty("HADOOP_USER_NAME", "test");



            DataStream<String> dataStream = env.addSource(sourceFunction, entry.getKey());
            DataStream<String> filterDataStream = dataStream.filter(new BinlogFilterFunction());
            DataStream<StatementBean> flatMapDataStream = filterDataStream.flatMap(new BinlogFlatMapFunction(active));
            flatMapDataStream.print();
            flatMapDataStream.addSink(new MySqlJdbcSink(active));
        }
    }
}
