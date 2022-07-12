package com.csfrez.flink.cdc.tool;

import com.alibaba.druid.pool.DruidDataSource;
import com.csfrez.flink.cdc.config.DruidConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DruidDataSourceTool {

    private static Map<String, DruidDataSource> dataSourceMap = new ConcurrentHashMap<>();

    public static DruidDataSource init(String name, String active) throws SQLException {
        Map<String, DruidConfig> druidConfigMap = DruidConfig.getDruidConfig(active);
        DruidConfig druidConfig = druidConfigMap.get(name);
        if(druidConfig != null){
            DruidDataSource dataSource = new DruidDataSource();
            dataSource.setDriverClassName(druidConfig.getDriverClassName());
            dataSource.setUrl(druidConfig.getUrl());
            dataSource.setUsername(druidConfig.getUsername());
            dataSource.setPassword(druidConfig.getPassword());
            // 初始化时建立物理连接的个数。初始化发生在显示调用init方法，或者第一次getConnection时
            dataSource.setInitialSize(1);
            // 最小连接池数量
            dataSource.setMinIdle(1);
            // 最大连接池数量
            dataSource.setMaxActive(200);
            dataSource.setMaxWait(1000 * 20);
            // 有两个含义：1) Destroy线程会检测连接的间隔时间2) testWhileIdle的判断依据，详细看testWhileIdle属性的说明
            dataSource.setTimeBetweenEvictionRunsMillis(1000 * 60);
            // 配置一个连接在池中最大生存的时间，单位是毫秒
            dataSource.setMaxEvictableIdleTimeMillis(1000 * 60 * 60 * 10);
            // 配置一个连接在池中最小生存的时间，单位是毫秒
            dataSource.setMinEvictableIdleTimeMillis(1000 * 60 * 60 * 9);
            // 这里建议配置为TRUE，防止取到的连接不可用
            dataSource.setTestWhileIdle(true);
            dataSource.setTestOnBorrow(true);
            dataSource.setTestOnReturn(false);
            dataSource.setValidationQuery("select 1");
            dataSource.init();
            return dataSource;
        }
        return null;
    }

    public static DruidDataSource getDruidDataSource(String name, String active) throws SQLException {
        DruidDataSource druidDataSource = dataSourceMap.get(name);
        if(druidDataSource == null) {
            synchronized (DruidDataSourceTool.class) {
                druidDataSource = dataSourceMap.get(name);
                if (druidDataSource == null) {
                    log.info("初始化数据源连接,数据源={},环境={}", name, active);
                    // log.info("" + LocalDateTime.now() + ",初始化数据源连接,数据源=" + name + ",环境="+ active);
                    druidDataSource = init(name, active);
                    if(druidDataSource != null){
                        dataSourceMap.put(name, druidDataSource);
                    }
                }
            }
        }
        return druidDataSource;
    }
}
