package com.csfrez.flink.cdc.dao;

import com.alibaba.druid.pool.DruidDataSource;
import com.csfrez.flink.cdc.config.DruidConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email csfrez@163.com
 */
public class DaoConnection {

    private static Map<String, DruidDataSource> dataSourceMap = new ConcurrentHashMap<>();


    private static DruidDataSource init(String name, String active) throws SQLException {
        Map<String, DruidConfig> druidConfigMap = DruidConfig.getDruidConfig(active);
        DruidConfig druidConfig = druidConfigMap.get(name);
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
        dataSource.setMaxActive(20);
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
        dataSourceMap.put(name, dataSource);
        return dataSource;
    }

    public synchronized static Connection getConnection(String name, String active) {
        DruidDataSource druidDataSource = dataSourceMap.get(name);
        try {
            if(druidDataSource != null){
                return druidDataSource.getConnection();
            }
            return init(name, active).getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        Connection connection = DaoConnection.getConnection("one", "test");
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT customer_name FROM orders WHERE order_id = ?");
            preparedStatement.setString(1, "10001");
            ResultSet resultSet = preparedStatement.executeQuery();
            String customerName = null;
            while (resultSet.next()) {
                customerName = resultSet.getString("customer_name");
            }
            System.out.println(customerName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println();
    }


}
