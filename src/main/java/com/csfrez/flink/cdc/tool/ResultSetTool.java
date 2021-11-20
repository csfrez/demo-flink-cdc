package com.csfrez.flink.cdc.tool;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.Date;

/**
 * @author yangzhi
 * @date 2021/11/16
 * @email csfrez@163.com
 */
@Slf4j
public class ResultSetTool {

    public static <T> T initBean(Class<T> clazz, String[] columns, ResultSet resultSet) throws Exception {
        T instance = clazz.newInstance();
        for(String fieldName: columns) {
            Field field = clazz.getDeclaredField(fieldName);
            Class<?> type = field.getType();
            if (type == String.class) {
                ReflectTool.setValueByFieldName(instance, field, resultSet.getString(fieldName));
            } else if (type == Long.class) {
                ReflectTool.setValueByFieldName(instance, field, resultSet.getLong(fieldName));
            } else if (type == Integer.class) {
                ReflectTool.setValueByFieldName(instance, field, resultSet.getInt(fieldName));
            } else if (type == BigDecimal.class) {
                ReflectTool.setValueByFieldName(instance, field, resultSet.getBigDecimal(fieldName));
            } else if (type == Date.class) {
                ReflectTool.setValueByFieldName(instance, field, resultSet.getTimestamp(fieldName));
            } else {
                ReflectTool.setValueByFieldName(instance, field, resultSet.getObject(fieldName));
            }
        }
        return instance;
    }

}
