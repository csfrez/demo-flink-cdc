package com.csfrez.flink.cdc.debezium;

import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.time.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.temporal.ChronoUnit;
import java.util.List;

@Slf4j
public class FastjonDeserializationSchema implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) {
        Struct dataRecord  =  (Struct)record.value();
        /**
         * 1、同时存在 beforeStruct 跟 afterStruct数据的话，就代表是update的数据
         * 2、只存在 beforeStruct 就是delete数据
         * 3、只存在 afterStruct数据 就是insert数据
         */

        JSONObject fastJson = new JSONObject();
        List<Field> fieldList;
        Struct beforeStruct = dataRecord.getStruct("before");
        if(beforeStruct != null){
            fieldList = beforeStruct.schema().fields();
            fastJson.put("before", this.formatJson(beforeStruct, fieldList));
        }
        Struct afterStruct = dataRecord.getStruct("after");
        if (afterStruct != null){
            fieldList = afterStruct.schema().fields();
            fastJson.put("after", this.formatJson(afterStruct, fieldList));
        }

        Struct source = dataRecord.getStruct("source");

        // 拿到databases table信息
        Object db = source.get("db");
        Object table = source.get("table");
        Object ts_ms = source.get("ts_ms");
        Object version = source.get("version");
        Object connector = source.get("connector");
        Object serverId = source.get("server_id");
        Object pos = source.get("pos");
        Object gtid = source.get("gtid");
        Object row = source.get("row");
        Object thread = source.get("thread");
        Object file = source.get("file");
        Object query = source.get("query");
        Object name = source.get("name");

        JSONObject sourceJson = new JSONObject();
        sourceJson.put("db", db);
        sourceJson.put("table", table);
        sourceJson.put("ts_ms", ts_ms);
        sourceJson.put("version", version);
        sourceJson.put("connector", connector);
        sourceJson.put("name", name);
        sourceJson.put("server_id", serverId);
        sourceJson.put("pos", pos);
        sourceJson.put("row", row);
        sourceJson.put("thread", thread);
        sourceJson.put("gtid", gtid);
        sourceJson.put("file", file);
        sourceJson.put("query", query);

        fastJson.put("source", sourceJson);
        fastJson.put("ts_ms", dataRecord.get("ts_ms"));
        fastJson.put("op", dataRecord.get("op"));
        fastJson.put("transaction", dataRecord.get("transaction"));
        String jsonString = JSONObject.toJSONString(fastJson);
        log.info(jsonString);
        out.collect(jsonString);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    private JSONObject formatJson(Struct struct, List<Field> fieldList){
        JSONObject jsonObject = new JSONObject();
        for (Field field : fieldList) {
            Schema schema = field.schema();
            String fieldName = field.name();
            Object fieldValue = struct.get(fieldName);
            if(Timestamp.SCHEMA_NAME.equals(schema.name()) && fieldValue != null){
                long epochMillis = Timestamp.toEpochMillis(fieldValue, temporal -> temporal.minus(8, ChronoUnit.HOURS));
                if(String.valueOf(epochMillis).equals(String.valueOf(fieldValue))){
                    epochMillis = epochMillis - 8*60*60*1000;
                }
                jsonObject.put(fieldName, epochMillis);
            } else {
                jsonObject.put(fieldName, fieldValue);
            }
        }
        return jsonObject;
    }
}
