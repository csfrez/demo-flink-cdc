package com.csfrez.flink.cdc.config;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Getter
@Setter
public class TableConfig {

    private static final String CONFIG_FILE = "table.json";

    private static Map<String, TableConfig> tableConfigMap = new ConcurrentHashMap<>();

    private String dataSourceName;
    private String columns;
    private String beanReference;
    private String relateTables;

    private TableConfig(){

    }

    static {
        try {
            String path = DruidConfig.class.getClassLoader().getResource(CONFIG_FILE).getPath();
            String json = FileUtils.readFileToString(new File(path), "UTF-8");
            System.out.println(json);
            JSONObject jsonObject = JSONObject.parseObject(json);
            Set<String> keySet = jsonObject.keySet();
            for(String key : keySet){
                TableConfig tableConfig = JSONObject.parseObject(JSONObject.toJSONString(jsonObject.get(key)), TableConfig.class);
                tableConfigMap.put(key, tableConfig);
            }
        } catch (Exception e) {
            log.error("initTableConfig()", e);
        }
    }

    /**
     * 获取所有的
     * @return
     */
    public static Map<String, TableConfig> getTableConfig(){
        return tableConfigMap;
    }

    public static TableConfig getTableConfig(String name){
        return tableConfigMap.get(name);
    }



    public static void main(String[] args) {
        System.out.println(getTableConfig("mydb.orders").getColumns());
    }


}
