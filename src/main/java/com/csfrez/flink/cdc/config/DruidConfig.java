package com.csfrez.flink.cdc.config;


import com.alibaba.fastjson.JSONObject;
import com.csfrez.flink.cdc.tool.HumpTool;
import com.csfrez.flink.cdc.tool.IOTool;
import com.csfrez.flink.cdc.tool.ReflectTool;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email csfrez@163.com
 */
@Slf4j
@Getter
@Setter
public class DruidConfig {

    private static boolean initFlag = false;
    private static Map<String, DruidConfig> configMap = new ConcurrentHashMap<>();

    private String url;

    private String driverClassName;

    private String username;

    private String password;

    private DruidConfig(){
    }

    public static Map<String, DruidConfig> getDruidConfig(String active){
        if(!initFlag){
            init(active);
        }
        return configMap;
    }

    public static void init(String active){
        System.out.println(Constant.DRUID_PREFIX + ".active=" + active);
        String configFile = Constant.DRUID_CONFIG_FILE;
        if(StringUtils.isNotEmpty(active)){
            configFile = Constant.DRUID_PREFIX + Constant.HYPHEN + active + Constant.PERIOD + Constant.SUFFIX;
        }
        InputStream in = null;
        try {
            Properties properties = new Properties();
            in = DruidConfig.class.getClassLoader().getResourceAsStream(configFile);
            properties.load(in);
            String[] names = properties.getProperty(Constant.DRUID_NAME).split(Constant.COMMA);
            for(String name : names){
                Field[] fields = DruidConfig.class.getDeclaredFields();
                DruidConfig config = new DruidConfig();
                for(Field field: fields){
                    String key = Constant.DRUID_PREFIX + Constant.PERIOD + name + Constant.PERIOD + HumpTool.humpToLine(field.getName());
                    String property = properties.getProperty(key);
                    if(StringUtils.isNotEmpty(property)){
                        System.out.println(Constant.DRUID_PREFIX + Constant.PERIOD + name + Constant.PERIOD + field.getName()+"=" + property);
                        ReflectTool.setValueByFieldName(config, field, property);
                    }
                }
                configMap.put(name, config);
            }
            initFlag = true;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("initDruidConfig()", e);
        } finally {
            IOTool.close(in);
        }
    }

    public static void main(String[] args) {
        //DruidConfig.init("dev");
        System.out.println(JSONObject.toJSONString(DruidConfig.getDruidConfig("")));
    }


}
