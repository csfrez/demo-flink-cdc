package io.debezium.time;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjuster;

/**
 * @author yangzhi
 * @date 2021/11/20
 * @email csfrez@163.com
 */
public class Timestamp {
    public static final String SCHEMA_NAME = "io.debezium.time.Timestamp";

    public static SchemaBuilder builder() {
        return SchemaBuilder.int64().name("io.debezium.time.Timestamp").version(1);
    }

    public static Schema schema() {
        return builder().build();
    }

    public static long toEpochMillis(Object value, TemporalAdjuster adjuster) {
        if (value instanceof Long) {
            return (Long)value;
        } else {
            LocalDateTime dateTime = Conversions.toLocalDateTime(value);
            if (adjuster != null) {
                dateTime = dateTime.with(adjuster);
            }
            return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            //return dateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
        }
    }

    private Timestamp() {
    }

}
