package com.csfrez.flink.cdc.debezium;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.time.Timestamp;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.temporal.ChronoUnit;
import java.util.List;


public class FastjonDeserializationSchema implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        Struct dataRecord  =  (Struct)record.value();
        /**
         * 1、同时存在 beforeStruct 跟 afterStruct数据的话，就代表是update的数据
         * 2、只存在 beforeStruct 就是delete数据
         * 3、只存在 afterStruct数据 就是insert数据
         */

        JSONObject fastJson = new JSONObject();
        List<Field> fieldList = null;
        Struct beforeStruct = dataRecord.getStruct("before");
        if(beforeStruct != null){
            JSONObject beforeJson = new JSONObject();
            fieldList = beforeStruct.schema().fields();
            for (Field field : fieldList) {
                Schema schema = field.schema();
                String fieldName = field.name();
                Object fieldValue = beforeStruct.get(fieldName);
                if(Timestamp.SCHEMA_NAME.equals(schema.name()) && fieldValue != null){
                    long epochMillis = Timestamp.toEpochMillis(fieldValue, temporal -> temporal.minus(8, ChronoUnit.HOURS));
                    if(String.valueOf(epochMillis).equals(String.valueOf(fieldValue))){
                        epochMillis = epochMillis - 8*60*60*1000;
                    }
                    beforeJson.put(fieldName, epochMillis);
                } else {
                    beforeJson.put(fieldName, fieldValue);
                }
            }
            fastJson.put("before", beforeJson);
        }
        Struct afterStruct = dataRecord.getStruct("after");
        if (afterStruct != null){
            JSONObject afterJson = new JSONObject();
            fieldList = afterStruct.schema().fields();
            for (Field field : fieldList) {
                Schema schema = field.schema();
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(fieldName);
                if(Timestamp.SCHEMA_NAME.equals(schema.name()) && fieldValue != null){
                    long epochMillis = Timestamp.toEpochMillis(fieldValue, temporal -> temporal.minus(8, ChronoUnit.HOURS));
                    if(String.valueOf(epochMillis).equals(String.valueOf(fieldValue))){
                        epochMillis = epochMillis - 8*60*60*1000;
                    }
                    afterJson.put(fieldName, epochMillis);
                } else {
                    afterJson.put(fieldName, fieldValue);
                }
            }
            fastJson.put("after", afterJson);
        }

        Struct source = dataRecord.getStruct("source");

        // 拿到databases table信息
        Object db = source.get("db");
        Object table = source.get("table");
        Object ts_ms = source.get("ts_ms");
        Object version = source.get("version");
        Object connector = source.get("connector");
        Object name = source.get("name");

        JSONObject sourceJson = new JSONObject();
        sourceJson.put("db", db);
        sourceJson.put("table", table);
        sourceJson.put("ts_ms", ts_ms);
        sourceJson.put("version", version);
        sourceJson.put("connector", connector);
        sourceJson.put("name", name);
        fastJson.put("source", sourceJson);

        fastJson.put("ts_ms", dataRecord.get("ts_ms"));
        fastJson.put("op", dataRecord.get("op"));
        fastJson.put("transaction", dataRecord.get("transaction"));

        out.collect(JSONObject.toJSONString(fastJson));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
