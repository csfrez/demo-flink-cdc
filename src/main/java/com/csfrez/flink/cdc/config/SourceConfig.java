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
public class SourceConfig {

    private static boolean initFlag = false;

    private static Map<String, SourceConfig> configMap = new ConcurrentHashMap<>();

    private String hostname;

    private Integer port = 3306;

    private String database;

    private String username;

    private String password;

    private SourceConfig(){
    }

    /**
     * 参数初始化
     * @param active
     */
    public static void init(String active){
        System.out.println(Constant.SOURCE_PREFIX + ".active=" + active);
        String configFile = Constant.SOURCE_CONFIG_FILE;
        if(StringUtils.isNotEmpty(active)){
            configFile = Constant.SOURCE_PREFIX + Constant.HYPHEN + active + Constant.PERIOD + Constant.SUFFIX;
        }
        InputStream in = null;
        try {
            Properties properties = new Properties();
            in = SourceConfig.class.getClassLoader().getResourceAsStream(configFile);
            properties.load(in);
            String[] names = properties.getProperty(Constant.SOURCE_NAME).split(Constant.COMMA);
            for(String name : names){
                Field[] fields = SourceConfig.class.getDeclaredFields();
                SourceConfig config = new SourceConfig();
                for(Field field: fields){
                    String key = Constant.SOURCE_PREFIX + Constant.PERIOD + name + Constant.PERIOD + HumpTool.humpToLine(field.getName());
                    String property = properties.getProperty(key);
                    if(StringUtils.isNotEmpty(property)){
                        System.out.println(Constant.SOURCE_PREFIX + Constant.PERIOD + name + Constant.PERIOD + field.getName()+"=" + property);
                        ReflectTool.setValueByFieldName(config, field, property);
                    }
                }
                configMap.put(name, config);
            }
            initFlag = true;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("initSourceConfig()", e);
        } finally {
            IOTool.close(in);
        }
    }

    public synchronized static Map<String, SourceConfig> getSourceConfig(){
        if(!initFlag){
            init("");
        }
        return configMap;
    }

    @Override
    public String toString() {
        return "SourceConfig{" +
                "hostname='" + hostname + '\'' +
                ", port=" + port +
                ", database='" + database + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }

    public static void main(String[] args) {
        //SourceConfig.init("dev");
        System.out.println(JSONObject.toJSONString(SourceConfig.getSourceConfig()));
    }


}
