package com.csfrez.flink.cdc.config;


import com.csfrez.flink.cdc.tool.HumpTool;
import com.csfrez.flink.cdc.tool.IOTool;
import com.csfrez.flink.cdc.tool.ReflectTool;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email csfrez@163.com
 */
@Slf4j
@Getter
@Setter
public class CommonConfig {

    private static final String PREFIX = "common.";
    private static final String CONFIG_FILE = "common.properties";
    private static Properties properties = new Properties();

    public static String sinkOutDatasource;

    private CommonConfig() {

    }

    static {
        InputStream in = null;
        try {
            in = CommonConfig.class.getClassLoader().getResourceAsStream(CONFIG_FILE);
            properties.load(in);
            init();
        } catch (Exception e) {
            log.error("CommonConfig()", e);
        } finally {
            IOTool.close(in);
        }
    }

    private static void init() throws Exception {
        Field[] fields = CommonConfig.class.getDeclaredFields();
        CommonConfig commonConfig = new CommonConfig();
        for (Field field : fields) {
            String property = properties.getProperty(PREFIX + HumpTool.humpToLine(field.getName()));
            if (StringUtils.isNotEmpty(property)) {
                log.info(PREFIX + field.getName() + "===>>" + property);
                ReflectTool.setValueByFieldName(commonConfig, field, property);
            }
        }
    }

    public static List<String> getSinkOutDatasourceList() {
        return Arrays.asList(sinkOutDatasource.split(","));
    }


    public static void main(String[] args) {
        System.out.println(CommonConfig.getSinkOutDatasourceList());
    }

}
