package com.csfrez.flink.cdc.dao;

import com.alibaba.druid.pool.DruidDataSource;
import com.csfrez.flink.cdc.config.DruidConfig;
import com.csfrez.flink.cdc.tool.DruidDataSourceTool;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class DaoConnection {

    public synchronized static Connection getConnection(String name, String active) {
        try {
            log.info("获取数据源连接,数据源={},环境={}", name, active);
            // log.info("" + LocalDateTime.now() + ",获取数据源连接,数据源=" + name + ",环境="+ active);
            DruidDataSource druidDataSource = DruidDataSourceTool.getDruidDataSource(name, active);
            if(druidDataSource != null){
                return druidDataSource.getConnection();
            }
        } catch (SQLException e) {
            log.error("DaoConnection", e);
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
