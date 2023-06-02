package com.csfrez.flink.cdc.tool;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;

/**
 * @author yangzhi
 * @date 2021/11/17
 * @email csfrez@163.com
 */
@Slf4j
public class ReflectTool {

    /**
     * 设置字段值
     * @param obj
     * @param field
     * @param value
     * @throws Exception
     */
    public static void setValueByFieldName(Object obj, Field field, Object value) throws Exception {
        if (field.isAccessible()) {
            //field.set(obj, value);
            setValue(obj, field, value, field.getType());
        } else {
            field.setAccessible(true);
            setValue(obj, field, value, field.getType());
            field.setAccessible(false);
        }
    }

    private static void setValue(Object obj, Field field, Object value, Class<?> clazz) throws IllegalAccessException {
        if(clazz == Integer.class){
            field.set(obj, Integer.parseInt(String.valueOf(value)));
        } else {
            field.set(obj, value);
        }
    }


}
