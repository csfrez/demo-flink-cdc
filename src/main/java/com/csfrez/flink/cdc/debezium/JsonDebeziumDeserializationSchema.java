package com.csfrez.flink.cdc.debezium;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.DecimalFormat;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverter;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterType;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private static final long serialVersionUID = 1L;
    private transient JsonConverter jsonConverter;
    private final Boolean includeSchema;

    public JsonDebeziumDeserializationSchema() {
        this(false);
    }

    public JsonDebeziumDeserializationSchema(Boolean includeSchema) {
        this.includeSchema = includeSchema;
    }

    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        if (this.jsonConverter == null) {
            this.jsonConverter = new JsonConverter();
            HashMap<String, Object> configs = new HashMap(4);
            configs.put("converter.type", ConverterType.VALUE.getName());
            configs.put("schemas.enable", this.includeSchema);
            configs.put("decimal.format", DecimalFormat.NUMERIC.name());
            this.jsonConverter.configure(configs);
        }

        byte[] bytes = this.jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        out.collect(new String(bytes));
    }

    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}