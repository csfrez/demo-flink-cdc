package com.csfrez.flink.cdc.config;


import com.alibaba.fastjson.JSONObject;
import com.csfrez.flink.cdc.tool.HumpTool;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email yangzhi@ddjf.com.cn
 */
@Slf4j
@Getter
@Setter
public class DruidConfig {

    private static final String PREFIX = "datasource.druid.";
    private static final String CONFIG_FILE = "druid.properties";
    private static Properties properties = new Properties();

    private String url;

    private String driverClassName;

    private String username;

    private String password;


    static {
        try {
            String path = DruidConfig.class.getClassLoader().getResource(CONFIG_FILE).getPath();
            InputStream in = new FileInputStream(path);
            properties.load(in);
        } catch (Exception e) {
            log.error("initDruidConfig()", e);
        }
    }

    public static Map<String, DruidConfig> getDruidConfig(){
        Map<String, DruidConfig> druidConfigMap = new ConcurrentHashMap<>();
        try {
            String[] names = properties.getProperty(PREFIX+"name").split(",");
            for(String name : names){
                Field[] fields = DruidConfig.class.getDeclaredFields();
                DruidConfig druidConfig = new DruidConfig();
                for(Field field: fields){
                    String property = properties.getProperty(PREFIX + name + "." + HumpTool.humpToLine(field.getName()));
                    if(StringUtils.isNotEmpty(property)){
                        System.out.println(PREFIX + name + "." + field.getName()+"=" + property);
                        setValueByFieldName(druidConfig, field, property);
                    }
                }
                druidConfigMap.put(name, druidConfig);
            }
        } catch (Exception e){
            log.error("getDruidConfig()", e);
        }
        return druidConfigMap;
    }

    public static void setValueByFieldName(Object obj, Field field, Object value) throws Exception {
        if (field.isAccessible()) {
            field.set(obj, value);
        } else {
            field.setAccessible(true);
            field.set(obj, value);
            field.setAccessible(false);
        }
    }

    public static void main(String[] args) {
        System.out.println(JSONObject.toJSONString(DruidConfig.getDruidConfig()));
    }


}
