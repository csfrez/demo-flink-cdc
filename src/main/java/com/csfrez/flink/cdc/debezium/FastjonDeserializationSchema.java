package com.csfrez.flink.cdc.debezium;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.util.List;

public class FastjonDeserializationSchema implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        Struct dataRecord  =  (Struct)record.value();
        Struct beforeStruct = dataRecord.getStruct("before");
        Struct afterStruct = dataRecord.getStruct("after");

        /**
         * 1、同时存在 beforeStruct 跟 afterStruct数据的话，就代表是update的数据
         * 2、只存在 beforeStruct 就是delete数据
         * 3、只存在 afterStruct数据 就是insert数据
         */

        JSONObject fastJson = new JSONObject();

        List<Field> fieldList = null;
        if(beforeStruct != null){
            JSONObject beforeJson = new JSONObject();
            fieldList = beforeStruct.schema().fields();
            for (Field field : fieldList) {
                String fieldName = field.name();
                Object fieldValue = beforeStruct.get(fieldName);
                beforeJson.put(fieldName,fieldValue);
            }
            fastJson.put("before", beforeJson);
        }
        if (afterStruct != null){
            JSONObject afterJson = new JSONObject();
            fieldList = afterStruct.schema().fields();
            for (Field field : fieldList) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(fieldName);
                afterJson.put(fieldName,fieldValue);
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
