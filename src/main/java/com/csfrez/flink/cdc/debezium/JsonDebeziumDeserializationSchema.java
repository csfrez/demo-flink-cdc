/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.csfrez.flink.cdc.debezium;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.DecimalFormat;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverter;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterConfig;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterType;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * A JSON format implementation of {@link DebeziumDeserializationSchema} which deserializes the
 * received {@link SourceRecord} to JSON String.
 */
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

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        if (jsonConverter == null) {
            // initialize jsonConverter
            jsonConverter = new JsonConverter();
            final HashMap<String, Object> configs = new HashMap<>(4);
            configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
            configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
            configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
            jsonConverter.configure(configs);
        }
        byte[] bytes = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        out.collect(new String(bytes));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
