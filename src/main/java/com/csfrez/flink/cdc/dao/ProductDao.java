package com.csfrez.flink.cdc.dao;

import com.csfrez.flink.cdc.bean.ProductBean;
import com.csfrez.flink.cdc.tool.IOTool;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class ProductDao extends BaseDao{

    private static final String TABLE_NAME = "mydb.products";

    public static ProductBean getProductBeanById(Long id){
        Connection connection = getConnection(TABLE_NAME);
        String sql = "SELECT id, name, description FROM products WHERE id = " + id;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(sql);
            ProductBean productBean = null;
            while(rs.next()){
                productBean = new ProductBean();
                productBean.setId(rs.getLong("id"));
                productBean.setName(rs.getString("name"));
                productBean.setDescription(rs.getString("description"));
            }
            return productBean;
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("getProductBeanById", e);
        } finally{
            IOTool.close(rs);
            IOTool.close(stmt);
        }
        return null;
    }

}
